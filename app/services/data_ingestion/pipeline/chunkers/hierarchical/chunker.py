# chunker.py — Stage 5: Section-Aware Chunking
#
#   Splits the document into sections first (using headings), then chunks
#   each section independently so a chunk never spans section boundaries.
#
#   - _split_into_sections() walks the heading list, slices raw_text between
#     consecutive headings, builds breadcrumbs by tracking active heading
#     levels, and populates HeadingNode.content as a side-effect.
#   - chunk_document() iterates sections and runs
#     RecursiveCharacterTextSplitter.split_text() on each section's text.
#   - Breadcrumb comes directly from the section — no char-offset search.

"""Stage 5 — Section-aware chunking with breadcrumb metadata.

Uses LangChain RecursiveCharacterTextSplitter + tiktoken.
"""

from dataclasses import dataclass

import tiktoken
from langchain_text_splitters import RecursiveCharacterTextSplitter

from app.settings import settings
from app.services.data_ingestion.pipeline.parsers.data_class.document import ParsedDocument, HeadingNode


@dataclass
class _Section:
    breadcrumb: str
    heading: HeadingNode | None
    text: str


def _split_into_sections(
    raw_text: str, headings: list[HeadingNode]
) -> list[_Section]:
    """Split raw_text at heading boundaries and build breadcrumbs.

    Also populates ``HeadingNode.content`` as a side-effect.
    """
    if not headings:
        return [_Section(breadcrumb="", heading=None, text=raw_text)]

    # Find the char position of each heading in raw_text (document order).
    heading_positions: list[tuple[int, HeadingNode]] = []
    search_start = 0
    for h in headings:
        idx = raw_text.find(h.title, search_start)
        if idx == -1:
            # Heading title not found — skip it; its text stays in the
            # preceding section.
            continue
        heading_positions.append((idx, h))
        search_start = idx + len(h.title)

    if not heading_positions:
        # None of the headings were found in the text.
        return [_Section(breadcrumb="", heading=None, text=raw_text)]

    sections: list[_Section] = []

    # --- Preamble: text before the first heading ---
    first_pos = heading_positions[0][0]
    preamble = raw_text[:first_pos].strip()
    if preamble:
        sections.append(_Section(breadcrumb="", heading=None, text=preamble))

    # --- Sections between headings ---
    active: dict[int, str] = {}

    for i, (pos, heading) in enumerate(heading_positions):
        # Update the active heading stack.
        active[heading.level] = heading.title
        # Clear deeper levels.
        for lvl in list(active):
            if lvl > heading.level:
                del active[lvl]

        breadcrumb = " > ".join(active[k] for k in sorted(active))

        # Section text runs from *after* the heading title to the start of
        # the next heading (or end of document).
        text_start = pos + len(heading.title)
        if i + 1 < len(heading_positions):
            text_end = heading_positions[i + 1][0]
        else:
            text_end = len(raw_text)

        section_text = raw_text[text_start:text_end].strip()

        # Populate HeadingNode.content.
        heading.content = section_text

        if not section_text:
            # Empty section (back-to-back headings) — skip.
            continue

        sections.append(
            _Section(breadcrumb=breadcrumb, heading=heading, text=section_text)
        )

    return sections


def chunk_document(doc: ParsedDocument) -> list[dict]:
    """Split a ParsedDocument into hierarchical parent + leaf chunks.

    Each section produces one parent chunk (concatenated text truncated to
    ``chunk_size`` tokens) and one or more leaf chunks (from the text
    splitter).  The parent dict carries a ``_children`` list of its leaf
    dicts so the orchestrator can persist them in two passes.

    Returns a list of parent dicts.  Each dict:
        {text, token_count, heading_breadcrumb, chunk_type, metadata, _children}
    Each child dict:
        {text, token_count, heading_breadcrumb, chunk_type, metadata}
    """
    enc = tiktoken.encoding_for_model("gpt-4")

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=settings.chunk_size,
        chunk_overlap=settings.chunk_overlap,
        length_function=lambda t: len(enc.encode(t)),
        separators=["\n\n", "\n", ". ", " ", ""],
    )

    sections = _split_into_sections(doc.raw_text, doc.headings)

    parents: list[dict] = []
    chunk_index = 0

    for section in sections:
        leaf_texts = splitter.split_text(section.text)

        # Single-leaf optimisation: emit parent-only, no children.
        # Avoids duplicate text/embeddings when the section fits in one chunk.
        if len(leaf_texts) == 1:
            token_count = len(enc.encode(leaf_texts[0]))
            parents.append({
                "text": leaf_texts[0],
                "token_count": token_count,
                "heading_breadcrumb": section.breadcrumb or None,
                "chunk_type": "parent",
                "metadata": {
                    "source": doc.source_filename,
                    "chunk_index": chunk_index,
                    "title": doc.title,
                    "page_count": doc.page_count,
                    "child_count": 0,
                },
                "_children": [],
            })
            chunk_index += 1
            continue

        # -- Build leaf dicts (multi-leaf sections only) --
        leaves: list[dict] = []
        for text in leaf_texts:
            token_count = len(enc.encode(text))
            leaves.append({
                "text": text,
                "token_count": token_count,
                "heading_breadcrumb": section.breadcrumb or None,
                "chunk_type": "leaf",
                "metadata": {
                    "source": doc.source_filename,
                    "chunk_index": chunk_index,
                    "title": doc.title,
                    "page_count": doc.page_count,
                },
            })
            chunk_index += 1

        # -- Build parent text: concatenate leaves, truncate to chunk_size --
        parent_text = "\n\n".join(t for t in leaf_texts)
        tokens = enc.encode(parent_text)
        if len(tokens) > settings.chunk_size:
            tokens = tokens[: settings.chunk_size]
            parent_text = enc.decode(tokens)

        parent_token_count = len(tokens)

        parents.append({
            "text": parent_text,
            "token_count": parent_token_count,
            "heading_breadcrumb": section.breadcrumb or None,
            "chunk_type": "parent",
            "metadata": {
                "source": doc.source_filename,
                "chunk_index": chunk_index,
                "title": doc.title,
                "page_count": doc.page_count,
                "child_count": len(leaves),
            },
            "_children": leaves,
        })
        chunk_index += 1

    return parents
