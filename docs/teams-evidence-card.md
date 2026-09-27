# The evidence card: what the edge draws from `AnswerPayload.evidence`

For the edge repository (`teams-agent-edge`).
The backend now sends the rows behind an answer as data, so the card can draw a real table instead of converting markdown.
Added on the backend side on 27 September 2026 as an optional field; the edge must mirror the contract and render it.

## Contract (mirror into `app/contracts.py`)

```python
class EvidenceColumn(BaseModel):
    name: str
    align: Literal["left", "right"] = "left"

class Evidence(BaseModel):
    title: str
    columns: list[EvidenceColumn]
    rows: list[list[str]]                # every cell already a string, numbers formatted
    facts: dict[str, str] = {}           # window, sorted by, groups, grain
    link: str | None = None              # the same view in the app, for an open-in button

class AnswerPayload(BaseModel):
    ...
    evidence: Evidence | None = None     # optional; schema_version stays 1
```

Example:

```json
{
  "title": "Top 10 by units short per customer name",
  "columns": [{"name": "customer name", "align": "left"}, {"name": "releases", "align": "right"}, {"name": "units short", "align": "right"}],
  "rows": [["SOUTHDOWNS MANOR", "13", "309"], ["GOODWOOD CLUB KENNELS", "13", "208"]],
  "facts": {"sorted by": "units short, biggest first", "groups": "10 of 79", "grain": "409 releases across the groups returned", "window": "2026-09-20 to 2026-09-27"},
  "link": "https://eye.example.com/matrix/releases"
}
```

## The card (Adaptive Card 1.5, `build_answer_card`)

Order of elements when `evidence` is present:

1. `TextBlock` heading: `evidence.title`, `weight: Bolder`, `size: Medium`.
2. `TextBlock` for `answer` with `wrap: true`.
   The backend already strips its own markdown table and ranked list from `answer` when evidence is present, so this is one or two sentences.
3. `Table`:
   - `columns`: one entry per `evidence.columns`; the first left-aligned column `{"width": "stretch"}` (names are long), every right-aligned column `{"width": "auto"}`.
   - `firstRowAsHeader: true`, header cells `weight: Bolder`, `size: Small`.
   - Body cells `TextBlock` with `wrap: true` for left columns and `wrap: false` for right columns, `horizontalAlignment` from `align`, `size: Small`.
   - `gridStyle: "default"`, `showGridLines: false`; give body rows alternating `style: "emphasis"` for zebra rows.
   - Cap at 25 rows (the backend sends at most 25).
4. `FactSet` from `evidence.facts`, in the order given, `spacing: Small`, `isSubtle: true` on the values.
5. `Action.OpenUrl` "Open in eSmart Eye" with `evidence.link` when present.
6. The existing footer facts (lookups, took) stay as they are.

When `evidence` is absent, keep today's markdown conversion unchanged.

## Why the text still contains a table

Clients that cannot render the card fall back to `answer`, which still carries the table as markdown.
A card must never show both: when `evidence` is present, do not run the markdown table conversion on `answer`; its pipe table has already been removed by the backend, and if one slips through, prefer the data.

## Semantic colour, optional

Adaptive Cards allow `color: "attention"` and `"warning"` on a TextBlock.
When a column is named `zero-pick`, colour non-zero cells `warning`; when `partial`, `attention`.
Never colour customer names or items; identity comes from the row, not a colour.
