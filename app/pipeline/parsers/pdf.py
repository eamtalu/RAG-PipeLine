"""Stage 3a — PDF parser using pdfplumber."""

import io
import re

import pdfplumber

from app.models.document import ParsedDocument, HeadingNode
from app.pipeline.parsers.base import BaseParser


class PdfParser(BaseParser):
    def parse(self, data: bytes, filename: str, mime_type: str) -> ParsedDocument:
        pages_text: list[str] = []
        headings: list[HeadingNode] = []

        with pdfplumber.open(io.BytesIO(data)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                pages_text.append(text)
                # Simple heading heuristic: lines in ALL CAPS or short bold-looking lines
                for line in text.split("\n"):
                    stripped = line.strip()
                    if stripped and stripped.isupper() and len(stripped) < 120:
                        headings.append(HeadingNode(level=1, title=stripped))

            page_count = len(pdf.pages)

        raw_text = "\n\n".join(pages_text)

        return ParsedDocument(
            source_filename=filename,
            mime_type=mime_type,
            title=self._extract_title(pages_text),
            raw_text=raw_text,
            headings=headings,
            page_count=page_count,
        )

    @staticmethod
    def _extract_title(pages: list[str]) -> str | None:
        if pages:
            first_line = pages[0].strip().split("\n")[0].strip()
            if first_line:
                return first_line[:256]
        return None
