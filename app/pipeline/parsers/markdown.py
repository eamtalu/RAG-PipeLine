"""Stage 3c — Markdown parser using mistune."""

import re

import mistune

from app.models.document import ParsedDocument, HeadingNode
from app.pipeline.parsers.base import BaseParser

_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+)$", re.MULTILINE)


class MarkdownParser(BaseParser):
    def parse(self, data: bytes, filename: str, mime_type: str) -> ParsedDocument:
        source = data.decode("utf-8", errors="replace")

        # Extract headings from raw markdown before rendering
        headings: list[HeadingNode] = []
        for match in _HEADING_RE.finditer(source):
            level = len(match.group(1))
            title = match.group(2).strip()
            headings.append(HeadingNode(level=level, title=title))

        # Render to HTML, then strip tags for plain text
        html = mistune.html(source)
        raw_text = self._strip_html(html)
        title = headings[0].title if headings else None

        return ParsedDocument(
            source_filename=filename,
            mime_type=mime_type,
            title=title,
            raw_text=raw_text,
            headings=headings,
        )

    @staticmethod
    def _strip_html(html: str) -> str:
        clean = re.sub(r"<[^>]+>", "", html)
        return re.sub(r"\n{3,}", "\n\n", clean).strip()
