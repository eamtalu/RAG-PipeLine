"""Abstract parser interface — every format parser implements this."""

from abc import ABC, abstractmethod

from app.models.document import ParsedDocument


class BaseParser(ABC):
    @abstractmethod
    def parse(self, data: bytes, filename: str, mime_type: str) -> ParsedDocument:
        """Parse raw file bytes into a normalised ParsedDocument."""
