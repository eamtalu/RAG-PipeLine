# pipeline_helper.py — Stage 4: Detection + Parsing Convenience Wrapper
#
#   A thin facade that chains two steps into one call:
#
#   normalize(data, filename) → detect_mime(data) → get_parser(mime) → parser.parse(data, ...) → ParsedDocument
#
#   The caller doesn't need to deal with detection and parser selection separately. This is the single entry point for "give me bytes, get back a structured document."

"""Stage 4 — Normalizer: detect → parse → return ParsedDocument.

All parsers already produce ParsedDocument, so the normalizer is a thin
convenience that chains detection and parsing into one call.
"""
import magic

from app.services.data_ingestion.pipeline.parsers.data_class.document import ParsedDocument
from app.services.data_ingestion.pipeline.parsers.ParserFactory import get_parser_for


def parse_document(data: bytes, filename: str) -> ParsedDocument:
    """Detect MIME, pick the right parser, and return a ParsedDocument."""
    mime_type = magic.from_buffer(data, mime=True)
    parser = get_parser_for(mime_type)
    doc = parser.parse(data, filename, mime_type)
    return doc
