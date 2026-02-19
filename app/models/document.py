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
