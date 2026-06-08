# base.py — Abstract Parser Interface
#
#   Defines the contract that every format-specific parser must follow:
#
#   - BaseParser is an abstract base class with a single method parse(data, filename, mime_type) → ParsedDocument
#   - This enforces a uniform API — the rest of the pipeline doesn't need to know which parser is running, just that it gets a ParsedDocument back
#   - Classic Template Method / Strategy pattern

"""Abstract parser interface — every format parser implements this."""

from abc import ABC, abstractmethod

from app.services.data_ingestion.pipeline.parsers.data_class.document import ParsedDocument


class BaseParser(ABC):
    @abstractmethod
    def parse(self, data: bytes, filename: str, mime_type: str) -> ParsedDocument:
        """Parse raw file bytes into a normalised ParsedDocument."""
