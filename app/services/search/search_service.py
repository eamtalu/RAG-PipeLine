"""Search service — hybrid search: vector similarity + keyword text matching.

The problem with pure vector search:
  Query: "what projects has Amin done at Infosapex"
  → embedding treats "Infosapex" as just another word in the semantic blob
  → returns chunks about "projects" that may not mention Infosapex at all

The fix — hybrid search:
  1. Extract entity keywords from the query (proper nouns, company names, etc.)
  2. Apply them as full-text match filters on Qdrant's TEXT-indexed fields
  3. Within those filtered chunks, rank by vector similarity

This gives: semantic understanding + hard entity constraints.
"""

import re

import openai

from app.settings import settings
from app.persistence.vectorstore import get_vector_store


# ── Common English words to ignore during entity extraction ──
_STOP_WORDS = frozenset({
    "i", "me", "my", "we", "our", "you", "your", "he", "she", "it", "they",
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
    "has", "have", "had", "do", "does", "did", "will", "would", "could",
    "should", "can", "may", "might", "shall", "not", "no", "what", "which",
    "who", "whom", "how", "when", "where", "why", "if", "then", "than",
    "that", "this", "these", "those", "about", "above", "after", "before",
    "between", "into", "through", "during", "up", "down", "out", "off",
    "over", "under", "again", "further", "once", "all", "any", "each",
    "every", "both", "few", "more", "most", "some", "such", "only", "own",
    "same", "so", "very", "just", "because", "as", "until", "while",
    "tell", "show", "list", "give", "find", "get", "describe", "explain",
    "done", "make", "made", "work", "worked", "experience", "project",
    "projects", "company", "role", "job", "position", "many", "much",
})


def extract_keywords(query: str) -> list[str]:
    """Extract entity keywords from a natural-language query.

    Heuristics:
      1. Capitalised words/phrases not at sentence start and not stop words
      2. CamelCase or ALL-CAPS tokens
      3. Words with special chars (e.g. "Node.js", "C++")

    Returns a list of keyword strings to use as text-match filters.
    """
    keywords: list[str] = []

    # Split into words, preserving punctuation attached to words
    tokens = query.split()

    i = 0
    while i < len(tokens):
        token = tokens[i]
        cleaned = re.sub(r'[?.!,;:"\']', '', token)

        if not cleaned:
            i += 1
            continue

        is_entity = False

        # Rule 1: ALL CAPS (2+ chars, not a stop word)
        if cleaned.isupper() and len(cleaned) >= 2 and cleaned.lower() not in _STOP_WORDS:
            is_entity = True

        # Rule 2: Capitalised word not at sentence start, not a stop word
        elif (cleaned[0].isupper() and cleaned.lower() not in _STOP_WORDS
              and not (i == 0 or tokens[i - 1].endswith(('.', '?', '!')))):
            # Greedily consume consecutive capitalised words (multi-word entity)
            parts = [cleaned]
            j = i + 1
            while j < len(tokens):
                next_cleaned = re.sub(r'[?.!,;:"\']', '', tokens[j])
                if next_cleaned and next_cleaned[0].isupper() and next_cleaned.lower() not in _STOP_WORDS:
                    parts.append(next_cleaned)
                    j += 1
                else:
                    break
            keywords.append(" ".join(parts))
            i = j
            continue

        # Rule 3: Contains dots/plus (e.g. "Node.js", "C++") — likely a tech term
        elif re.search(r'[.+#]', cleaned) and len(cleaned) >= 2:
            is_entity = True

        if is_entity:
            keywords.append(cleaned)

        i += 1

    return keywords


async def _embed_query(text: str) -> list[float]:
    client = openai.AsyncOpenAI(api_key=settings.openai_api_key)
    response = await client.embeddings.create(
        model=settings.embedding_model,
        input=[text],
        dimensions=settings.embedding_dimensions,
    )
    return response.data[0].embedding


async def search(
    query: str,
    top_k: int = 10,
    profile: str | None = None,
    document_type: str | None = None,
    section_root: str | None = None,
    section_parent: str | None = None,
    section_heading: str | None = None,
    chunk_type: str | None = None,
    job_id: str | None = None,
    keywords: list[str] | None = None,
) -> list[dict]:
    """Hybrid search: vector similarity + keyword text matching.

    Args:
        query:           Natural language query (embedded for vector search)
        keywords:        Explicit entity keywords for text matching.
                         If None, auto-extracted from the query.
                         Pass empty list [] to disable keyword extraction.
        profile..job_id: Exact-match metadata filters (KEYWORD-indexed)
        top_k:           Max results to return
    """
    vector = await _embed_query(query)

    # ── Exact-match filters (KEYWORD-indexed) ──
    exact_filter: dict[str, str] = {}
    if profile:
        exact_filter["profile"] = profile
    if document_type:
        exact_filter["document_type"] = document_type
    if section_root:
        exact_filter["section_root"] = section_root
    if section_parent:
        exact_filter["section_parent"] = section_parent
    if section_heading:
        exact_filter["section_heading"] = section_heading
    if chunk_type:
        exact_filter["chunk_type"] = chunk_type
    if job_id:
        exact_filter["job_id"] = job_id

    # ── Text-match filters (TEXT-indexed) — entity keywords ──
    if keywords is None:
        keywords = extract_keywords(query)

    # Join all keywords into a single text match query
    # Each keyword must appear in the chunk text
    text_match: dict[str, str] = {}
    if keywords:
        # Apply each keyword as a separate text match condition (AND logic via must)
        # Qdrant MatchText does tokenized word match, so multi-word entities
        # are matched as "all tokens must appear"
        text_match = {f"text": " ".join(keywords)}

    store = get_vector_store()
    raw_results = await store.query(
        vector=vector,
        top_k=top_k,
        filter=exact_filter if exact_filter else None,
        text_match=text_match if text_match else None,
    )

    results = []
    for r in raw_results:
        meta = r["metadata"]
        results.append({
            "chunk_id": r["id"],
            "score": r["score"],
            "text": r["text"],
            "profile": meta.get("profile"),
            "document_type": meta.get("document_type"),
            "section_root": meta.get("section_root"),
            "section_parent": meta.get("section_parent"),
            "section_heading": meta.get("section_heading"),
            "context_path": meta.get("context_path"),
            "context_depth": meta.get("context_depth"),
            "heading_breadcrumb": meta.get("heading_breadcrumb"),
            "page_numbers": meta.get("page_numbers"),
            "chunk_type": meta.get("chunk_type"),
            "token_count": meta.get("token_count"),
            "job_id": meta.get("job_id"),
            "source_file": meta.get("source_file"),
        })

    return results, keywords
