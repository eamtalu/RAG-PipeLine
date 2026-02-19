"""Stage 3d — HTML parser using BeautifulSoup4."""

from bs4 import BeautifulSoup

from app.models.document import ParsedDocument, HeadingNode
from app.pipeline.parsers.base import BaseParser

_HEADING_TAGS = {"h1": 1, "h2": 2, "h3": 3, "h4": 4, "h5": 5, "h6": 6}


class HtmlParser(BaseParser):
    def parse(self, data: bytes, filename: str, mime_type: str) -> ParsedDocument:
        soup = BeautifulSoup(data, "html.parser")

        # Extract title
        title_tag = soup.find("title")
        title = title_tag.get_text(strip=True) if title_tag else None

        # Extract headings
        headings: list[HeadingNode] = []
        for tag_name, level in _HEADING_TAGS.items():
            for tag in soup.find_all(tag_name):
                headings.append(HeadingNode(level=level, title=tag.get_text(strip=True)))

        # Plain text extraction
        raw_text = soup.get_text(separator="\n", strip=True)

        return ParsedDocument(
            source_filename=filename,
            mime_type=mime_type,
            title=title,
            raw_text=raw_text,
            headings=headings,
        )
