"""Carry a transaction's identity across the rebuild that Stage 2 performs on it.

`_txn_id` derives a transaction's id from its CONTENT - the hash of its REQUEST entry, or of its
earliest entry when it has none. That is stable only while the content is, and `regroup_window`
deletes transactions and rebuilds them from whatever entries are present at that moment. A backfilled
file that adds an earlier line, or supplies the REQUEST line that failed to parse, changes the anchor
and therefore the id. Measured on production: 1.3% of transactions are exposed to this.

An id that changes is not an identity. Everything that remembered it - notification dedupe, alert deep
links, agent citations, saved frontend links - is silently wrong afterwards.

So identity is assigned ONCE and then carried. `log_entry_assignment` already records which entries
belonged to which transaction; `regroup_window` simply discards that a moment before it would answer
the question. Read it first, and each rebuilt group can name the transaction it came from: the one
that owned the plurality of its entries.

Merges and splits both fall out of the plurality rule. A merge keeps the larger contributor's id; a
split keeps it for the larger half and the remainder mints a fresh one.

This module is deliberately pure - it takes a decision, it does no I/O, and it knows nothing about
`LogEntry` beyond `.id`. The read that feeds it is `assignments.load_owners_in_window`, and the
fallback for genuinely new groups is `derive_transactions._txn_id`, unchanged. That fallback is what
makes the whole change safe to deploy: no id that exists today is rewritten.
"""

import uuid
from collections import Counter
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

#: A read-only empty mapping, so `EMPTY` below is safe to share as a default argument. A plain `{}`
#: default would be one mutable object reachable from every call site that omits the parameter.
_NO_OWNERS: Mapping[uuid.UUID, uuid.UUID] = MappingProxyType({})


@dataclass(frozen=True)
class Continuity:
    """Which transaction owned each entry before the rebuild freed it.

    `reusable` is the safety boundary, and it is not decoration. The unique constraint on
    `log_transactions` is `UNIQUE NULLS NOT DISTINCT (id, started_at)` rather than unique on `id`,
    because a partitioned table requires the partition key inside it. So Postgres will NOT reject a
    second row that shares an id but differs in `started_at` - it simply lands in another partition
    and two rows claim one identity, silently and undetectably after the fact.

    `regroup_window` deletes with `WHERE id IN (freed)` and no `started_at` bound, so a freed id is
    definitely gone and safe to reuse. Any other id belongs to a row that still exists. Restricting
    reuse to `reusable` is what keeps that distinction, and it costs one set lookup per entry.
    """

    #: {entry_id: transaction_id}, read in ONE bulk query before the delete. The factory hands back
    #: the SAME read-only proxy every time; `dataclasses` rejects an unhashable value as a bare
    #: default, and sharing it is only safe because it cannot be written to.
    owner_by_entry: Mapping[uuid.UUID, uuid.UUID] = field(default_factory=lambda: _NO_OWNERS)

    #: The transactions being rebuilt. Only these ids may be handed back out.
    reusable: frozenset[uuid.UUID] = frozenset()

    def owner_of(self, entry) -> uuid.UUID | None:
        """The transaction this entry belonged to, if that transaction is safe to reuse.

        The guard is applied per ENTRY rather than per group on purpose: a group can hold entries
        from both a freed transaction and a live one, and only the freed one may cast a vote.
        """
        owner = self.owner_by_entry.get(entry.id)
        return owner if owner in self.reusable else None


#: No predecessors - every group mints a fresh id, which is exactly today's behaviour. The default for
#: every path except `regroup_window`, so adding continuity at one call site cannot change what the
#: others do. Safe to share because both fields are immutable.
EMPTY = Continuity()


def _votes(entries, continuity: Continuity) -> Counter:
    """How many of these entries each reusable predecessor owned.

    Entries with no owner, and entries whose owner is not safe to reuse, cast no vote - `owner_of`
    collapses both cases to None, so the distinction never has to be repeated here.
    """
    votes: Counter = Counter()
    for entry in entries:
        owner = continuity.owner_of(entry)
        if owner is not None:
            votes[owner] += 1
    return votes


def _by_votes_then_id(item: tuple[uuid.UUID, int]) -> tuple[int, bytes]:
    """Rank a predecessor: most entries wins, ties broken on the id itself.

    The tiebreak exists so a rebuild is reproducible. `regroup_all` advertises idempotency, and an
    identity that fell out of dict iteration order would make that claim false.
    """
    owner, votes = item
    return votes, owner.bytes


def _plurality_owner(entries, continuity: Continuity) -> tuple[uuid.UUID, int] | None:
    """(transaction_id, votes) for the transaction owning most of these entries, or None."""
    votes = _votes(entries, continuity)
    return max(votes.items(), key=_by_votes_then_id) if votes else None


def _claims(groups, fallbacks: list[uuid.UUID], continuity: Continuity) -> list[tuple]:
    """Every group's bid for a predecessor's id, strongest first.

    Sorted on (-votes, fallback id) so the group with the most inherited entries is served first, and
    two equal bids resolve the same way on every run. The fallback id is a stable, membership-derived
    tiebreak that is already being computed.
    """
    claims = []
    for i, entries in enumerate(groups):
        winner = _plurality_owner(entries, continuity)
        if winner is not None:
            owner, votes = winner
            claims.append((-votes, fallbacks[i].bytes, i, owner))
    return sorted(claims)


def _award(claims: list[tuple]) -> dict[int, uuid.UUID]:
    """{group index: inherited id}, giving each id to at most ONE group.

    The single-award rule is what a split needs. Handing the same id to both halves would write
    `(id, started_a)` and `(id, started_b)` - accepted by the constraint, since the `started_at`
    differ - and leave two rows sharing an identity with nothing to distinguish them.
    """
    taken: set[uuid.UUID] = set()
    awarded: dict[int, uuid.UUID] = {}
    for _votes, _tiebreak, index, owner in claims:
        if owner not in taken:
            taken.add(owner)
            awarded[index] = owner
    return awarded


def assign(groups, continuity: Continuity, *, fallback) -> list[uuid.UUID]:
    """One id per group: inherited where there is a predecessor, freshly minted where there is not.

    `fallback` is `derive_transactions._txn_id`, injected rather than imported so this module stays
    free of the pipeline it serves - and so a test can state what "a new id" means without dragging in
    the anchor rules.
    """
    fallbacks = [fallback(entries) for entries in groups]
    awarded = _award(_claims(groups, fallbacks, continuity))
    return [awarded.get(i, fallbacks[i]) for i in range(len(groups))]
