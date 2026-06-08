# html.py — Stage 3d: HTML Parser
#
#   Parses HTML files using BeautifulSoup4:
#
#   - Title — finds the <title> tag and extracts its text
#   - Headings — finds all <h1> through <h6> tags and builds HeadingNode objects with the correct level
#   - Text extraction — soup.get_text(separator="\n", strip=True) gives clean plain text with tags removed
#   - The simplest parser since HTML already has semantic structure

"""Stage 3d — HTML parser using BeautifulSoup4."""

from bs4 import BeautifulSoup

from app.services.data_ingestion.pipeline.parsers.data_class.document import ParsedDocument, HeadingNode
from app.services.data_ingestion.pipeline.parsers.base import BaseParser

_HEADING_TAGS = {"h1": 1, "h2": 2, "h3": 3, "h4": 4, "h5": 5, "h6": 6}


class HtmlParser(BaseParser):
    def parse(self, data: bytes, filename: str, mime_type: str) -> ParsedDocument:
        soup = BeautifulSoup(data, "html.parser")

        # Extract title
        title_tag = soup.find("title")
        title = title_tag.get_text(strip=True) if title_tag else None

        # Extract headings in document order
        headings: list[HeadingNode] = [
            HeadingNode(level=_HEADING_TAGS[tag.name], title=tag.get_text(strip=True))
            for tag in soup.find_all(list(_HEADING_TAGS))
        ]

        # Plain text extraction
        raw_text = soup.get_text(separator="\n", strip=True)

        return ParsedDocument(
            source_filename=filename,
            mime_type=mime_type,
            title=title,
            raw_text=raw_text,
            headings=headings,
        )
