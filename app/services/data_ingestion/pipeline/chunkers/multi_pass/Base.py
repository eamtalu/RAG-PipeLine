"""Abstract base for all multi-pass chunkers."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from app.services.data_ingestion.pipeline.parsers.pdf import RawLine


@dataclass
class ChunkResult:
    """Output of a multi-pass chunker — maps 1:1 to ChunkEntity fields."""
    chunk_id: str
    text: str
    context_path: list
    context_header: str
    full_text: str
    chunk_type: str
    page_numbers: list
    token_estimate: int
    metadata: dict = field(default_factory=dict)


class BaseChunker(ABC):
    @abstractmethod
    def chunk(self, lines: list[RawLine], doc_meta: dict) -> list[ChunkResult]:
        """Chunk extracted PDF lines into a list of ChunkResults."""
