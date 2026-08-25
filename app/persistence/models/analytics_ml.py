"""M1. The two tables the ML pipeline owns.

The plan says "features plus the PINNED REVISION and a code version". That phrasing is corrected here,
because a revision is not a usable coordinate: `analytics_fact_ledger.revision` is PER FACT - measured
1..2 per transaction - and the ledger does not carry the tenant-level revision at all. There is no
global revision number to pin to.

The coordinate is an INSTANT, `pinned_at`, resolved against `analytics_fact_ledger.recorded_at`. That
works because a fold stamps every ledger row it writes with the same `recorded_at`, so a fold is atomic
in those terms and any instant is a clean cut between folds. Ties break on `revision` descending, so
even a same-microsecond collision resolves deterministically.

What makes a training set reproducible
--------------------------------------
`analytics_facts` holds only the latest version of each fact, so a training set built from it is
un-rebuildable the moment anything is restated. `analytics_fact_ledger` holds EVERY version, which is
why F10 insisted it exist "from day one rather than being added when ML starts".

So a feature set stores three things and is reproducible from them alone:

    pinned_at      which versions of the facts to use
    code_version   which feature engineering produced them
    content_hash   what the answer was, so a rebuild can be CHECKED rather than trusted

Without the third, "reproducible" is a claim. With it, it is a test.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class AnalyticsFeatureSet(Base):
    """One training set: which facts, which code, and what it came to."""

    __tablename__ = "analytics_feature_sets"
    __table_args__ = (
        # A name plus a pin plus a code version identifies a training set completely. Re-requesting the
        # same three must return the SAME set rather than build a second one, which is what makes a
        # training run repeatable instead of merely repeatable-looking.
        UniqueConstraint("customer_code", "name", "pinned_at", "code_version",
                         name="uq_analytics_feature_sets_pin"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)

    #: The coordinate. Every fact is taken at its newest ledger version whose `recorded_at` is at or
    #: before this instant - so the same pin selects the same versions forever, however many times the
    #: facts are restated afterwards.
    pinned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    #: Which feature engineering produced this. Bumped when the transformation changes, because the same
    #: facts through different code are a different training set and must not share an identity.
    code_version: Mapped[str] = mapped_column(String(32), nullable=False)

    #: What the answer was. A rebuild at the same pin recomputes this and compares - which turns
    #: "reproducible" from a claim into something a test can fail on.
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    row_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    #: The feature names, in order. Stored because a set whose columns cannot be identified is not a
    #: training set, and because a reader must be able to tell a schema change from a data change.
    feature_names: Mapped[list] = mapped_column(JSONB, default=list, server_default="[]")

    #: The rows themselves are NOT stored here. They are a pure function of `(pinned_at, code_version)`
    #: over the ledger, so storing them would be a second copy that can disagree with the first - and it
    #: would be the largest table in the system. The hash is what makes recomputation checkable.
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    built_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
                                               default=lambda: datetime.now(timezone.utc))


class AnalyticsPrediction(Base):
    """One model output, keyed by what it is about, how far ahead, and which model said so."""

    __tablename__ = "analytics_predictions"
    __table_args__ = (
        # All four are needed. The same subject at the same horizon from two model versions is two
        # predictions to compare, not a conflict - which is the whole point of keeping a model version.
        UniqueConstraint("customer_code", "subject", "horizon", "model_version", "target_at",
                         name="uq_analytics_predictions_key"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    #: What the prediction is about - an item number, a warehouse, an operator. A free string rather
    #: than a typed reference, because the subject of a forecast is chosen per model and a column per
    #: possible subject would be a schema change every time somebody has an idea.
    subject: Mapped[str] = mapped_column(String(128), nullable=False)
    subject_kind: Mapped[str] = mapped_column(String(32), nullable=False, default="item_number")

    #: How far ahead, as a label rather than an interval: "7d", "1m". A label because it is a
    #: DIMENSION people group by, and because two models may define "next week" differently.
    horizon: Mapped[str] = mapped_column(String(16), nullable=False)

    model_version: Mapped[str] = mapped_column(String(64), nullable=False)

    #: The instant this is a prediction FOR, and the instant it was made. Both, because a forecast is
    #: only assessable against the two together - and back-testing needs to know what was knowable when.
    target_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    predicted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
                                                   default=lambda: datetime.now(timezone.utc))

    value: Mapped[object | None] = mapped_column(Numeric(20, 6), nullable=True)
    #: Additive components rather than a finished interval, matching invariant 8's reasoning: a stored
    #: confidence interval cannot be re-aggregated, whereas the pieces it is built from can.
    detail: Mapped[dict] = mapped_column(JSONB, default=dict, server_default="{}")

    #: Which training set the model behind this was built from. Lineage is the point: a prediction whose
    #: training data cannot be identified cannot be explained when it is wrong.
    feature_set_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
                                                 default=lambda: datetime.now(timezone.utc))
