# The edge/backend wire contract: QuestionJob, AnswerPayload and the answer blocks.
# Copied from the Teams edge repo (app/core/contracts_dto.py) by scripts/sync_contract.sh;
# edit it there, run scripts/contract_schema.py --write, then sync. Both repos test the snapshot.
"""The two messages that cross the edge/backend boundary.

These models are the contract with the backend worker (RAG FAST API, `teams_question_worker`).
Both sides validate against them, so a field added here must be optional or bumped in
`schema_version`. Keep this file dependency-free apart from pydantic so the backend can copy it
verbatim.
"""

from datetime import datetime, timezone
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, model_validator

#: Logged by both sides at start-up, so an operator can see which revision each runs. The
#: schema_version fields below stay: every addition since v2 is optional, so an older side
#: ignores what it does not know and a newer side fills in defaults.
CONTRACT_REVISION = "3.17"


def _now() -> datetime:
    return datetime.now(timezone.utc)


# These are the pydantic models used in requests, responses and external messages


#: The access-control actions of the Admin tab (step 7c). Each carries its fields in `payload`:
#: access_set_user {object_id, display_name?, email?, role?, all_sites?, site_ids?, status?,
#:   starts_on?, note?, capabilities?} (creates the user when new, else changes them);
#: access_deactivate_user {object_id}; access_set_matrix_cell {role, capability, state};
#: access_decide {reference, approve: bool, changes?, note?};
#: access_request {capability} or {site_ids} (the person asking for more, for themselves);
#: access_undo {reference} or {object_id} (a receipt's undo, within its window; step 8);
#: advisory_act {advisory_id | advisory_ids: [..50], action: acknowledge | assign | snooze | dismiss |
#:   resolve, assignee?, minutes? | until?} (the Advisories tab's buttons; step 9; step 17 acts on a
#:   selection or a group at once, each advisory checked and audited on its own).
#: task_create {kind: reminder | schedule, title, when: spec, note?, site_id?, query_text?, skip_if?,
#:   target?: me | channel, show_as?: table | chart | summary, until?, source?, via?} (step 10)
#: task_update {task_id, action: pause | resume | run_now | done | skip | stop | delete | edit | plan_again
#:   | copy | repeat, fields?, note?, when?} (step 15: run_now runs the routine by hand and records the run;
#:   done and skip record the person's own outcome with a note; plan_again books the same routine at a
#:   new `when`; copy makes a new one from it with `fields` on top; repeat turns a one-off into a
#:   recurring one with `when`)
#: group_set {group_id?, name, topics, min_severity, channel_ids, members, max_per_hour?} (step 12;
#:   max_per_hour 1..60 is step 13, the cards a group sends per clock hour) ; group_delete {group_id}
#: group_join {group_id} ; group_leave {group_id}
#: channel_set {channel_id?, name, webhook_url?, channel_type?}
#: channel_delete {channel_id} ; channel_test {channel_id}
#: watch_add {kind: delivery | route | location | zone | product | measure, entity_id, alert?, until?:
#:   end_of_day | 7_days | iso, swap_watch_id?, via?} (step 14; a full list answers with a choice block
#:   whose options carry the swap) ; watch_update {watch_id, action: set_alert | resume, alert?, until?}
#: watch_stop {watch_id, via?} ; watch_seen {event_ids?} ; watch_not_now {kind, entity_id}
#: demo_act {action_id, form_id, values} (step 18: a confirmed demo form; one audit line marked demo,
#:   nothing else changes, the receipt's status is "demo"). A confirmed form of any kind may also
#:   arrive as {form_id, values, via}: the service unwraps it into its own fields (step 18).
ACCESS_COMMAND_KINDS = (
    "access_set_user",
    "access_deactivate_user",
    "access_set_matrix_cell",
    "access_decide",
    "access_request",
    "access_undo",
    "advisory_act",
    "demo_act",
    "task_create",
    "task_update",
    "group_set",
    "group_delete",
    "group_join",
    "group_leave",
    "channel_set",
    "channel_delete",
    "channel_test",
    "watch_add",
    "watch_update",
    "watch_stop",
    "watch_seen",
    "watch_not_now",
)


class Command(BaseModel):
    """An action the tab asks the backend to perform instead of a question: marking a delivery on
    the at-risk board as checked (or taking that back), and since step 7c the access-control
    actions of the Admin tab. Rides on a QuestionJob so it reuses the queue, the consumer and the
    answer poll; the consumer never runs the agent for one. Added on the backend side on
    2026-10-02 and mirrored here; both files must stay identical."""

    kind: Literal[
        "at_risk_check",
        "at_risk_uncheck",
        "access_set_user",
        "access_deactivate_user",
        "access_set_matrix_cell",
        "access_decide",
        "access_request",
        "access_undo",
        "advisory_act",
        "demo_act",
        "task_create",
        "task_update",
        "group_set",
        "group_delete",
        "group_join",
        "group_leave",
        "channel_set",
        "channel_delete",
        "channel_test",
        "watch_add",
        "watch_update",
        "watch_stop",
        "watch_seen",
        "watch_not_now",
    ]
    #: The at-risk fields; required for the two at-risk kinds, checked by the backend's runner.
    delivery_number: str | None = Field(default=None, max_length=64)
    departure_date: str | None = Field(
        default=None, description="YYYY-MM-DD, when the delivery has more than one row."
    )
    by: str | None = Field(
        default=None,
        max_length=128,
        description="The Teams display name of the person acting.",
    )
    note: str | None = Field(default=None, max_length=1000)
    #: The access actions' fields (step 7c); empty for the at-risk kinds.
    payload: dict[str, Any] = Field(default_factory=dict)


class QuestionJob(BaseModel):
    """One question from Teams, published by the edge to SQS and consumed by the backend worker."""

    schema_version: int = 2
    job_id: str = Field(..., description="Edge-generated UUID; the worker echoes it in the answer.")
    tenant_id: str = Field(..., description="Entra tenant id of the customer, from Microsoft.")
    customer_code: str = Field(
        ..., description="Backend log space resolved from the tenant binding."
    )
    conversation_id: str = Field(
        ...,
        description="Teams conversation id; the reply goes here and "
        "conversation memory is keyed by it.",
    )
    activity_id: str | None = Field(default=None, description="Teams activity id of the question.")
    sender_object_id: str = Field(
        ...,
        min_length=1,
        description="Entra object id of the sender. Required since contract v2: the per-user "
        "limits and roles key on it, so a job that cannot say who asked is malformed.",
    )
    sender_name: str | None = None
    site_id: str | None = Field(
        default=None,
        max_length=16,
        description="The site the page was looking at when the question was asked (step 6); "
        "absent from the bot and from older pages. Shown on the history, never a filter.",
    )
    question: str = Field(..., min_length=1)
    #: What the question is about, when the page knows (3.11): {"advisory_id": ...} from "Ask why";
    #: the backend frames the question with it so the agent reads the advisory first. Since 3.14 also
    #: {"choice": {"question": ..., "text": ...}} when the person typed an answer to a choice block.
    context: dict[str, Any] | None = Field(default=None)
    enqueued_at: datetime = Field(default_factory=_now)
    source: Literal["bot", "tab"] = Field(
        default="bot",
        description="Where the question was typed: the Teams chat, or the Home tab's "
        "chat pane (answered by storing the payload on the job rather than sending it).",
    )
    command: Command | None = Field(
        default=None,
        description="Set when the job is an action, not a question; absent for a question.",
    )


class EvidenceColumn(BaseModel):
    name: str
    align: Literal["left", "right"] = "left"


class Evidence(BaseModel):
    """The rows behind the answer, as data, so the edge draws a real table on the card instead of
    guessing widths from markdown. Optional: absent for answers that rest on no rows. Added on the
    backend side on 2026-09-27 and mirrored here; both files must stay identical."""

    title: str
    columns: list[EvidenceColumn]
    rows: list[list[str]]
    facts: dict[str, str] = Field(
        default_factory=dict,
        description="Small labelled facts under the table: window, sorted by, grain.",
    )
    link: str | None = Field(
        default=None, description="Where the same view lives in the app, for an open-in button."
    )


# ---------------------------------------------------------------------------- answer blocks
# An answer is text plus an ordered list of typed blocks the page renders with code it ships
# (DESIGN.md "Ask response contract", built in stage D step 5). The model never emits HTML: the
# backend lifts these out of its presentation tool calls, validates them, and drops what fails.
# The edge drops one unreadable block, never the answer (`app/core/answer_parsing.py`).

KNOWN_CHART_KINDS = frozenset({"bar", "line", "pie"})
_NAME = r"^[a-z][a-z0-9_]{0,31}$"


class TextBlock(BaseModel):
    type: Literal["text"] = "text"
    markdown: str = Field(..., max_length=4000)


class TableBlock(BaseModel):
    type: Literal["table"] = "table"
    title: str = Field(..., max_length=120)
    columns: list[EvidenceColumn] = Field(..., min_length=1, max_length=12)
    rows: list[list[str]] = Field(..., max_length=25)

    @model_validator(mode="after")
    def _rows_fit_columns(self) -> "TableBlock":
        """Every row has one cell per column, so the page never draws a ragged table.

        (2 columns, rows of 2) -> ok ; (a row of 3) -> ValueError
        """
        width = len(self.columns)
        if any(len(row) != width for row in self.rows):
            raise ValueError(f"every row must have {width} cells")
        return self


class ChartSeries(BaseModel):
    name: str = Field(..., max_length=60)
    values: list[float] = Field(..., min_length=1, max_length=48)


class ChartReference(BaseModel):
    """A horizontal line the chart draws, such as a target."""

    label: str = Field(..., max_length=40)
    value: float


class ChartBlock(BaseModel):
    type: Literal["chart"] = "chart"
    kind: str = Field(..., pattern=r"^[a-z][a-z_]{1,15}$")  # bar | line | pie; unknown = dropped
    title: str = Field(..., max_length=120)
    labels: list[str] = Field(..., min_length=1, max_length=48)
    series: list[ChartSeries] = Field(..., min_length=1, max_length=6)
    unit: str | None = Field(default=None, max_length=24)
    stacked: bool = False
    #: Series named on_time / late / left_behind are drawn in the status colours.
    status: bool = False
    reference: ChartReference | None = None

    @model_validator(mode="after")
    def _shape_holds(self) -> "ChartBlock":
        """The numbers line up with the labels, a pie is one series of non-negative shares, and the
        kind is one the page can draw.

        (bar, 3 labels, series of 3) -> ok ; (series of 2) -> ValueError ; (pie, 2 series) -> ValueError
        """
        if self.kind not in KNOWN_CHART_KINDS:
            raise ValueError(f"unknown chart kind {self.kind!r}")
        for s in self.series:
            if len(s.values) != len(self.labels):
                raise ValueError(
                    f"series {s.name!r} has {len(s.values)} values for {len(self.labels)} labels"
                )
        if self.kind == "pie":
            if len(self.series) != 1:
                raise ValueError("a pie has exactly one series")
            if any(v < 0 for v in self.series[0].values):
                raise ValueError("a pie has no negative values")
        return self


class FormField(BaseModel):
    name: str = Field(..., pattern=_NAME)
    label: str = Field(..., max_length=80)
    kind: Literal["text", "number", "date", "select", "checkbox"] = "text"
    options: list[str] = Field(default_factory=list, max_length=24)
    #: The words shown for each option (step 18), when the options themselves are ids; same length
    #: as options or empty.
    labels: list[str] = Field(default_factory=list, max_length=24)
    required: bool = True
    placeholder: str | None = Field(default=None, max_length=80)
    value: str | None = Field(default=None, max_length=200)  # what the assistant pre-filled


class FormBlock(BaseModel):
    """A form the person confirms before anything changes (step 8 builds the submit)."""

    type: Literal["form"] = "form"
    form_id: str = Field(..., pattern=_NAME)
    title: str = Field(..., max_length=120)
    fields: list[FormField] = Field(..., min_length=1, max_length=12)
    requires_approval: bool = False
    submit_label: str = Field(default="Submit", max_length=40)
    note: str | None = Field(default=None, max_length=300)
    #: The command a confirmed form becomes (step 18), sent as {form_id, values, via}; absent on a
    #: form from before, which the page maps by its form_id as it always did.
    command_kind: str | None = Field(default=None, max_length=32)
    #: A demo form (step 18) changes nothing when confirmed; the page says so beside its title.
    mode: Literal["real", "demo"] = "real"


class ReceiptBlock(BaseModel):
    """What happened after a confirmation, with a reference and a time-limited undo."""

    type: Literal["receipt"] = "receipt"
    text: str = Field(..., max_length=300)
    reference: str | None = Field(default=None, max_length=40)
    status: str = Field(default="done", pattern=r"^(done|needs_approval|refused|demo)$")
    undo_until: datetime | None = None
    #: What the page sends as the `access_undo` command's payload while undo_until has not passed
    #: (step 8); absent when nothing can be undone.
    undo: dict[str, Any] | None = None


class FollowupsBlock(BaseModel):
    type: Literal["followups"] = "followups"
    items: list[str] = Field(..., min_length=1, max_length=4)


#: The watchlist blocks (step 14). An entity is a watchable thing an answer was built on (from the
#: query results, never parsed out of the text); the page draws it as a chip with the eye toggle.
class EntityRef(BaseModel):
    kind: Literal["delivery", "route", "location", "zone", "product", "measure"]
    id: str = Field(..., max_length=128)
    label: str = Field(..., max_length=200)
    watched: bool = False
    can_watch: bool = True
    site_id: str | None = Field(default=None, max_length=16)


class EntitiesBlock(BaseModel):
    type: Literal["entities"] = "entities"
    items: list[EntityRef] = Field(..., min_length=1, max_length=8)


class WatchSuggestionBlock(BaseModel):
    """At most one per answer: an offer to watch the thing the question was about."""

    type: Literal["watch_suggestion"] = "watch_suggestion"
    entity: EntityRef
    text: str = Field(..., max_length=300)
    default_alert: str = Field(..., max_length=120)


class ChoiceOption(BaseModel):
    id: str = Field(..., max_length=128)
    label: str = Field(..., max_length=200)
    detail: str = Field(default="", max_length=200)
    suggested: bool = False
    #: A ready command the page runs when the option is tapped (kind + payload), so a tap never
    #: goes back through the model; absent for an option that only closes the question.
    command: dict[str, Any] | None = None


class ChoiceBlock(BaseModel):
    """One question with answers to tap (and a typed answer when allowed): which of two zones,
    what level, which watch to swap out."""

    type: Literal["choice"] = "choice"
    question: str = Field(..., max_length=300)
    options: list[ChoiceOption] = Field(..., min_length=1, max_length=12)
    allow_typed: bool = True


class TopicQuestion(BaseModel):
    """One starter of a topic (step 18): the sentence to tap, and whether it asks or acts."""

    text: str = Field(..., max_length=160)
    kind: Literal["question", "action", "demo"] = "question"
    needs: str | None = Field(default=None, max_length=32)


class Topic(BaseModel):
    """One topic card of the Ask catalogue (step 18)."""

    id: str = Field(..., pattern=_NAME)
    label: str = Field(..., max_length=40)
    blurb: str = Field(..., max_length=120)
    icon: str = Field(..., max_length=24)
    needs: str | None = Field(default=None, max_length=32)
    questions: list[TopicQuestion] = Field(..., min_length=1, max_length=12)


class TopicsBlock(BaseModel):
    """The "What can I ask" answer (step 18): the topics the asker may use, drawn as cards."""

    type: Literal["topics"] = "topics"
    topics: list[Topic] = Field(..., max_length=12)


Block = Annotated[
    TextBlock
    | TableBlock
    | ChartBlock
    | FormBlock
    | ReceiptBlock
    | FollowupsBlock
    | EntitiesBlock
    | WatchSuggestionBlock
    | ChoiceBlock
    | TopicsBlock,
    Field(discriminator="type"),
]


class AnswerPayload(BaseModel):
    """The worker's answer, POSTed to the edge at /internal/answers."""

    schema_version: int = 1
    job_id: str
    conversation_id: str
    status: Literal["ok", "error"] = "ok"
    answer: str = Field(..., description="Markdown text. On status=error a short human sentence.")
    cited_transaction_ids: list[str] = Field(default_factory=list)
    tool_call_count: int = 0
    duration_seconds: float | None = None
    evidence: Evidence | None = None
    # --- stage D step 5: the typed blocks and the times (all optional, schema_version stays 1)
    blocks: list[Block] = Field(default_factory=list, max_length=8)
    asked_at: datetime | None = None
    answered_at: datetime | None = None
    #: When the figures are from (the tenant's analytics watermark), and whether they still move.
    data_as_of: datetime | None = None
    provisional: bool = False
