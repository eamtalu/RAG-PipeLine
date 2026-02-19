"""Stage 4 — Normalizer: detect → parse → return ParsedDocument.

All parsers already produce ParsedDocument, so the normalizer is a thin
convenience that chains detection and parsing into one call.
"""

from app.models.document import ParsedDocument
from app.pipeline.detector import detect_mime, get_parser


def normalize(data: bytes, filename: str) -> ParsedDocument:
    """Detect MIME, pick the right parser, and return a ParsedDocument."""
    mime_type = detect_mime(data)
    parser = get_parser(mime_type)
    doc = parser.parse(data, filename, mime_type)
    return doc
