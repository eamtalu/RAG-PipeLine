"""Stage 2 — True MIME detection via python-magic + strategy routing."""

import magic

from app.pipeline.parsers.base import BaseParser
from app.pipeline.parsers.pdf import PdfParser
from app.pipeline.parsers.docx import DocxParser
from app.pipeline.parsers.markdown import MarkdownParser
from app.pipeline.parsers.html import HtmlParser

# Strategy map: MIME type → parser class
_PARSER_REGISTRY: dict[str, type[BaseParser]] = {
    "application/pdf": PdfParser,
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": DocxParser,
    "text/markdown": MarkdownParser,
    "text/plain": MarkdownParser,  # treat plain text as markdown
    "text/html": HtmlParser,
}


def detect_mime(data: bytes) -> str:
    """Sniff the true MIME type from file bytes (not extension)."""
    mime = magic.from_buffer(data, mime=True)
    # python-magic may return 'text/plain' for .md files — that's fine,
    # the registry maps both text/plain and text/markdown to MarkdownParser.
    return mime


def get_parser(mime_type: str) -> BaseParser:
    """Route to the correct parser using the strategy pattern."""
    parser_cls = _PARSER_REGISTRY.get(mime_type)
    if parser_cls is None:
        raise ValueError(f"Unsupported MIME type: {mime_type}")
    return parser_cls()
