"""The range diff: compare a whole event-time range, reverse what left, apply what changed.

Phase 3 of docs/analytics-ml-architecture/final_architecture.md. **Pure: no database, no clock.**

    Range diff, never per-record update. ... A merged record's vanished id reverses, a split's new id
    applies. A per-record update passes test 3 and fails this one.

This is the most expensive decision in the plan, and the reason it is worth the cost fits in a sentence:
a per-id upsert never asks about an id the source no longer mentions. Stage 2 rebuilds are free to merge
two transactions into one or split one into two, so ids genuinely vanish -- and when one does, its
contribution stays in every total permanently and nothing raises. Measured on the fixtures, the merge
case lands on exactly double.

Comparing the whole range instead means a vanished id, a merge, a split, a delete and a tenant purge are
all the same branch: *stored here, absent from the source, so reverse it*. None of them is special-cased,
which is the argument for the range diff in one line.

Three properties make it safe to run repeatedly, and each buys something concrete:

*Idempotent.* A ticket stays open when a cycle fails and is retried, so the retry must be free.

*Order-independent.* Both sides are read with no ORDER BY, so PostgreSQL may hand them over however it
likes.

*Scoped on BOTH sides.* The diff only ever touches keys whose `event_time` lies in the range it was
given, because both the stored rows and the source rows were read with the same predicate. That is what
lets two adjacent day-chunks of one ticket be processed in either order without one undoing the other:
a transaction that moved across the boundary is outside the other chunk's range on both sides, so that
chunk cannot see it, let alone reverse it.

Identity is `(source_transaction_id, event_time)`, never the id alone (F3). `log_transactions` enforces
`UNIQUE NULLS NOT DISTINCT (id, started_at)` because `started_at` is the partition key and is nullable,
so two rows may share an id across partitions. Keying on the id alone would merge them and lose one.
"""

import enum
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

#: The identity of a fact, as a hashable pair.
Key = tuple[str, datetime | None]

#: A signed contribution: `(+1, row)` to fold in, `(-1, row)` to take back out.
Delta = tuple[int, Mapping[str, Any]]


class Action(enum.Enum):
    """What the diff decided about one key. Four outcomes, and `unchanged` is the important one.

    `unchanged` is not an absence of work, it is a positive finding: the fingerprint matched, so this
    key must be left completely alone. At a 98.7% rebuild rate it is the common path, and treating it
    as "update to the same value" would turn every recheck into a write.
    """

    insert = "insert"        # in the source, not stored
    update = "update"        # stored and in the source, fingerprints differ
    unchanged = "unchanged"  # stored and in the source, fingerprints match: write nothing
    reverse = "reverse"      # stored, absent from the source: merge, split, delete or purge


@dataclass(frozen=True)
class Outcome:
    """One key's verdict, carrying both sides so the caller needs no second lookup.

    `stored` is kept on an update precisely because the reversal delta must use it: the stored row is
    what was folded IN, so it is the only thing whose subtraction cancels.
    """

    action: Action
    key: Key
    #: The new value. None only for a reversal, where there is no new value by definition.
    fact: Mapping[str, Any] | None
    #: What was already there. None only for an insert.
    stored: Mapping[str, Any] | None

    @property
    def writes(self) -> bool:
        """Whether this outcome touches the database at all."""
        return self.action is not Action.unchanged


def _as_utc(value: Any) -> datetime | None:
    """A timestamp normalised so the two sides of the diff hash the same.

    A naive value out of asyncpg is a UTC instant; reading it as local time would put the stored row
    and the source row in different keys and turn one transaction into a reversal plus an insert.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None \
            else value.astimezone(timezone.utc)
    return value


def key_of(row: Mapping[str, Any]) -> Key:
    """`row`'s identity: both columns (F3).

    The id is stringified because the same value arrives as `uuid.UUID` from the database and as a
    string from a fixture or an API payload, and `UUID(x) != str(x)` -- which would make a stored row
    and its own source row look like different keys and reverse-then-reinsert every one of them.
    """
    txn_id = row.get("source_transaction_id")
    ident = str(txn_id) if isinstance(txn_id, (uuid.UUID, str)) else str(txn_id)
    return ident, _as_utc(row.get("event_time"))


def _by_key(rows: Iterable[Mapping[str, Any]]) -> dict[Key, Mapping[str, Any]]:
    return {key_of(r): r for r in rows}


def diff(stored: Sequence[Mapping[str, Any]],
         source: Sequence[Mapping[str, Any]]) -> list[Outcome]:
    """What to do about every key in `stored` or `source`, for one already-scoped range.

    Both arguments must have been read with the SAME range predicate over `event_time`. Passing a
    wider source than stored would reverse nothing that left; passing a wider stored than source would
    reverse rows that are merely outside the window and still perfectly valid -- the loudest possible
    version of leak F12, self-inflicted.
    """
    have, want = _by_key(stored), _by_key(source)
    outcomes: list[Outcome] = []

    for key, row in want.items():
        old = have.get(key)
        if old is None:
            outcomes.append(Outcome(Action.insert, key, row, None))
        elif old.get("source_version_hash") == row.get("source_version_hash"):
            # The fingerprint matched. Carry the STORED row forward, not the incoming one: the stored
            # row holds its revision and its `created_at`, and `created_at` is what F6's retention
            # cursor reads. Swapping in an identical-looking incoming row would reset both and drag
            # the cursor backwards.
            outcomes.append(Outcome(Action.unchanged, key, old, old))
        else:
            outcomes.append(Outcome(Action.update, key, row, old))

    # Stored but absent from the source. One branch for a merge's vanished id, a split's, a date-range
    # delete, a full wipe and a tenant purge -- none of them special-cased.
    for key, old in have.items():
        if key not in want:
            outcomes.append(Outcome(Action.reverse, key, None, old))

    return outcomes


def deltas(outcomes: Iterable[Outcome]) -> list[Delta]:
    """The signed contributions the rollups must apply, in the order they should be read.

    A reversal is emitted BEFORE its application for an update. The arithmetic does not care, but a
    ledger showing the new value before the old one was taken back reads as though the total briefly
    doubled, and that is the reading someone will do at 2am.
    """
    out: list[Delta] = []
    for o in outcomes:
        if o.action is Action.unchanged:
            continue
        if o.stored is not None:
            out.append((-1, o.stored))
        if o.fact is not None and o.action is not Action.reverse:
            out.append((1, o.fact))
    return out
