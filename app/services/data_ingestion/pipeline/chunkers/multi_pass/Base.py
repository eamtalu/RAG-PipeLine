from abc import ABC,abstractmethod

from app.persistence.models.document import ParsedDocument


class BaseChunker(ABC):
    @abstractmethod
    def chunk(self, doc_profile : str, text : str) -> str:
        """Chunk text based on the profile CV"""