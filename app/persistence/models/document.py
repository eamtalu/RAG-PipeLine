# document.py — Parser Output Contract (Pydantic, not a DB table)
#
#   This is not a database table — it's a Pydantic model that defines the normalized output format every file parser must produce. It's the contract between the parsing stage and
#   the chunking stage.
#
#   Key design intentions:
#   - HeadingNode — a recursive tree structure representing the document's heading hierarchy (H1 → H2 → H3, etc.), enabling heading-breadcrumb-aware chunking
#   - ParsedDocument — uniform representation regardless of source format (PDF, DOCX, HTML, etc.), containing:
#     - raw_text — full extracted text for chunking
#     - headings — structural tree for smart chunking
#     - page_count, metadata — extra info passed along the pipeline

"""Stage 4 — Normalised ParsedDocument that every parser must produce."""

from pydantic import BaseModel, Field


class HeadingNode(BaseModel):
    level: int = Field(..., ge=1, le=6)
    title: str
    content: str = ""
    children: list["HeadingNode"] = Field(default_factory=list)


class ParsedDocument(BaseModel):
    """Uniform representation produced by every format-specific parser."""

    source_filename: str
    mime_type: str
    title: str | None = None
    raw_text: str = Field(..., description="Full extracted plain text")
    headings: list[HeadingNode] = Field(default_factory=list, description="Heading tree for breadcrumb-aware chunking")
    page_count: int | None = None
    metadata: dict = Field(default_factory=dict)
