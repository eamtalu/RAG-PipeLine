# markdown.py — Stage 3c: Markdown Parser
#
#   Parses Markdown (and plain text) using mistune:
#
#   - Heading extraction — uses a regex (^#{1,6}\s+(.+)$) on the raw markdown source before rendering, extracting heading levels from the # count
#   - Text extraction — renders the markdown to HTML via mistune.html(), then strips all HTML tags to get clean plain text
#   - Collapses excessive newlines (\n{3,} → \n\n) for tidiness
#   - Also handles text/plain files (the detector routes both text/markdown and text/plain here)

"""Stage 3c — Markdown parser using mistune."""

import re

import mistune

from app.services.data_ingestion.pipeline.parsers.data_class.document import ParsedDocument, HeadingNode
from app.services.data_ingestion.pipeline.parsers.base import BaseParser

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
