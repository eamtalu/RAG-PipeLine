"""Search endpoint — hybrid search: vector similarity + keyword text matching.

How it works:
  1. Entity keywords are auto-extracted from the query (or you can pass them explicitly)
  2. Keywords become hard text-match filters on the chunk text (must contain the word)
  3. Vector similarity ranks the filtered chunks by semantic relevance

Example — "what projects has Amin done at Infosapex":
  → auto-extracts ["Infosapex"] as keyword
  → only searches chunks containing "Infosapex"
  → ranks those by semantic similarity to the full query
"""

from pydantic import BaseModel, Field

from fastapi import APIRouter

from app.services.search.search_service import search

router = APIRouter(prefix="/documents", tags=["documents"])


class SearchRequest(BaseModel):
    query: str
    top_k: int = Field(default=10, ge=1, le=50)

    # Exact-match filters (KEYWORD-indexed)
    profile: str | None = None
    document_type: str | None = None
    section_root: str | None = None
    section_parent: str | None = None
    section_heading: str | None = None
    chunk_type: str | None = None
    job_id: str | None = None

    # Keyword text-match filter (TEXT-indexed) — hybrid search
    # None  = auto-extract entity keywords from query
    # []    = disable keyword extraction, pure vector search
    # ["Infosapex", "Python"] = explicit keywords, chunks must contain these
    keywords: list[str] | None = None


class SearchResult(BaseModel):
    chunk_id: str
    score: float
    text: str
    profile: str | None = None
    document_type: str | None = None
    section_root: str | None = None
    section_parent: str | None = None
    section_heading: str | None = None
    context_path: list[str] | None = None
    context_depth: int | None = None
    heading_breadcrumb: str | None = None
    page_numbers: list[int] | None = None
    chunk_type: str | None = None
    token_count: int | None = None
    job_id: str | None = None
    source_file: str | None = None


class SearchResponse(BaseModel):
    results: list[SearchResult]
    total: int
    keywords_used: list[str]  # shows which keywords were applied as filters


@router.post("/search", response_model=SearchResponse)
async def search_documents(body: SearchRequest):
    results, keywords_used = await search(
        query=body.query,
        top_k=body.top_k,
        profile=body.profile,
        document_type=body.document_type,
        section_root=body.section_root,
        section_parent=body.section_parent,
        section_heading=body.section_heading,
        chunk_type=body.chunk_type,
        job_id=body.job_id,
        keywords=body.keywords,
    )
    return SearchResponse(
        results=results,
        total=len(results),
        keywords_used=keywords_used,
    )
