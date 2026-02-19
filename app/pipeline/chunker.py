"""Stage 5 — Heading-aware chunking with breadcrumb metadata.

Uses LangChain RecursiveCharacterTextSplitter + tiktoken.
"""

import tiktoken
from langchain_text_splitters import RecursiveCharacterTextSplitter

from app.config import settings
from app.models.document import ParsedDocument, HeadingNode


def _build_breadcrumb(headings: list[HeadingNode], char_offset: int, raw_text: str) -> str:
    """Walk the heading list and return e.g. 'Chapter 1 > Section A'."""
    active: dict[int, str] = {}
    pos = 0
    for h in headings:
        idx = raw_text.find(h.title, pos)
        if idx == -1 or idx > char_offset:
            break
        active[h.level] = h.title
        # Clear deeper levels when a higher-level heading appears
        for lvl in list(active):
            if lvl > h.level:
                del active[lvl]
        pos = idx + len(h.title)
    return " > ".join(active[k] for k in sorted(active))


def chunk_document(doc: ParsedDocument) -> list[dict]:
    """Split a ParsedDocument into chunks with breadcrumb metadata.

    Returns a list of dicts: {"text": str, "token_count": int, "metadata": dict}
    """
    enc = tiktoken.encoding_for_model("gpt-4")

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=settings.chunk_size,
        chunk_overlap=settings.chunk_overlap,
        length_function=lambda t: len(enc.encode(t)),
        separators=["\n\n", "\n", ". ", " ", ""],
    )

    texts = splitter.split_text(doc.raw_text)

    chunks: list[dict] = []
    search_start = 0
    for i, text in enumerate(texts):
        token_count = len(enc.encode(text))

        # Find approximate char offset for breadcrumb lookup
        offset = doc.raw_text.find(text[:60], search_start)
        if offset == -1:
            offset = search_start
        breadcrumb = _build_breadcrumb(doc.headings, offset, doc.raw_text)
        search_start = offset + len(text) // 2

        chunks.append({
            "text": text,
            "token_count": token_count,
            "heading_breadcrumb": breadcrumb or None,
            "metadata": {
                "source": doc.source_filename,
                "chunk_index": i,
                "title": doc.title,
                "page_count": doc.page_count,
            },
        })

    return chunks
