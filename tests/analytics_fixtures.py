"""The nine Phase 0 fixtures. Not a test file: data, imported by the tests beside it.

docs/analytics-ml-architecture/final_architecture.md, Phase 0:

    Fixtures for zero pick, short pick, error, incomplete, late backfill, rebuild, **merge**, **split**,
    and a multi-confirmation line whose `ExpectedQuantity` changes.

Every fixture is a PAIR of states: what the projection said before, and what it says now. That shape is
not incidental. `log_transactions` is a mutable derived projection -- 98.7% of rows are rewritten after
they are first written -- so every interesting case is a transition, and a fixture that described only
one state could not express the two that matter.

Merge and split are the two the design turns on. Verification item 4:

    A merged record's vanished id reverses, a split's new id applies.
    A per-record update passes test 3 and fails this one.

That is the claim the tests beside this file prove, by running both strategies over all nine and
showing which ones a per-id upsert gets wrong. It is worth proving in Phase 0 rather than trusting,
because the range diff is the most expensive decision in the plan and this is the evidence for it.

Quantities are the shapes the live server actually emits: `30.0` from QuantityPicked, `30.000000` from
ExpectedQuantity, and genuinely fractional values like `0.333333`.
"""

from dataclasses import dataclass
from decimal import Decimal

from app.services.analytics import contract as c

#: A fact row as N2 will hand it over: quantity already Decimal, classification already decided.
#: `version` is the fingerprint that lets a recheck be absorbed as a no-op -- at a 98.7% rebuild rate
#: almost every one must be, or the system writes a constant stream of pointless aggregate updates.
def fact(txn_id: str, method: str, quantity, *, version: str = "v1",
         transaction_name: str = "Pick", status: str = "success") -> dict:
    qty = c.parse_quantity(quantity) if quantity is not None else None
    return {
        "source_transaction_id": txn_id,
        "source_version_hash": version,
        "method": method,
        "transaction_name": transaction_name,
        "status": status,
        "quantity": qty,
        "quantity_classification": c.classify(method, qty),
    }


@dataclass(frozen=True)
class Fixture:
    """One before/after transition, and what it is here to catch."""

    name: str
    catches: str
    before: tuple[dict, ...]
    after: tuple[dict, ...]

    @property
    def ids_before(self) -> set:
        return {r["source_transaction_id"] for r in self.before}

    @property
    def ids_after(self) -> set:
        return {r["source_transaction_id"] for r in self.after}

    @property
    def vanished(self) -> set:
        """Ids the projection no longer has. Non-empty for merge and split, and the only thing a
        per-id update can never see."""
        return self.ids_before - self.ids_after


PICK = "ConfirmPickLine"
COUNT = "ReportCount"
LIST = "ListPickLines"


FIXTURES: tuple[Fixture, ...] = (

    Fixture(
        name="zero pick",
        catches="F8. 1,333 of 16,075 live pick confirmations record zero units and still report "
                "success, typically an empty location. Counting them as picks makes the zero-pick "
                "rate nothing, and that rate is a first-class metric because it names the location.",
        before=(),
        after=(fact("t-zero", PICK, "0.0"),),
    ),

    Fixture(
        name="short pick",
        catches="A partial pick is a real pick, not a failure. ExpectedQuantity arrives as 30.000000 "
                "and QuantityPicked as 12.0; comparing them as strings reports every pick as short.",
        before=(),
        after=(fact("t-short", PICK, "12.0"),),
    ),

    Fixture(
        name="error",
        catches="A hard failure carries no units. It must not appear in the quantity total, and must "
                "not be silently folded in as a zero-unit attempt either.",
        before=(),
        after=(fact("t-err", PICK, None, status="error"),),
    ),

    Fixture(
        name="incomplete",
        catches="A REQUEST whose RESPONSE has not arrived. Its quantity is unknown, NOT zero. This is "
                "the row that is still due to move: measured, sealed rows averaged 1.7h between their "
                "newest entry and being written.",
        before=(),
        after=(fact("t-inc", PICK, None, status="incomplete"),),
    ),

    Fixture(
        name="late backfill",
        catches="An older transaction appearing after newer ones were already folded. The ticket range "
                "must cover it; a watermark that only moved forward would skip it forever.",
        before=(fact("t-new", PICK, "5.0"),),
        after=(fact("t-old", PICK, "2.0"), fact("t-new", PICK, "5.0")),
    ),

    Fixture(
        name="rebuild",
        catches="The same transaction rewritten with a different quantity: the ordinary case at a "
                "98.7% rebuild rate. The old contribution must reverse exactly once. THIS is the "
                "fixture a per-id update passes, which is why it cannot be the only one.",
        before=(fact("t-rb", PICK, "5.0", version="v1"),),
        after=(fact("t-rb", PICK, "3.0", version="v2"),),
    ),

    Fixture(
        name="rebuild, unchanged",
        catches="A recheck that changed nothing. The fingerprint matches, so it must write NOTHING. "
                "At a 98.7% rebuild rate this is the common path, and if it is not free the worker "
                "produces a constant stream of pointless aggregate writes.",
        before=(fact("t-same", PICK, "4.0", version="v1"),),
        after=(fact("t-same", PICK, "4.0", version="v1"),),
    ),

    Fixture(
        name="merge",
        catches="Two transactions become one, so an id VANISHES. A per-id update never looks for the "
                "departed id, so its contribution stays in the total permanently and no error is "
                "raised. This is the fixture that justifies the range diff.",
        before=(fact("t-m1", PICK, "2.0"), fact("t-m2", PICK, "3.0")),
        after=(fact("t-merged", PICK, "5.0"),),
    ),

    Fixture(
        name="split",
        catches="One transaction becomes two, so an id vanishes and two appear. The mirror image of "
                "merge, and wrong in the same silent way under a per-id update.",
        before=(fact("t-s", PICK, "7.0"),),
        after=(fact("t-s1", PICK, "4.0"), fact("t-s2", PICK, "3.0")),
    ),

    Fixture(
        name="multi-confirmation, ExpectedQuantity changes",
        catches="The ground truth's trap: ExpectedQuantity is mutable per instruction, not an "
                "order-line total, so fill rate is not derivable from it. Two confirmations against "
                "one line, the expectation revised between them, and only the PICKED quantities may "
                "be summed. Fractional on purpose: 0.333333 is a real live value.",
        before=(fact("t-c1", PICK, "0.333333", version="v1"),),
        after=(fact("t-c1", PICK, "0.333333", version="v2"),
               fact("t-c2", PICK, "0.666667", version="v1")),
    ),
)


#: Rows for a definition that has nothing to do with quantities, to keep the fixtures honest about the
#: 46 of 49 methods whose measures are volume, duration, status and actor.
NON_QUANTITY_ROWS: tuple[dict, ...] = (
    fact("t-l1", LIST, None, transaction_name="List"),
    fact("t-l2", LIST, None, transaction_name="List"),
)

BY_NAME: dict[str, Fixture] = {f.name: f for f in FIXTURES}
