"""Abstract vector store interface — plug in any backend."""

from abc import ABC, abstractmethod


class VectorStore(ABC):
    @abstractmethod
    async def ensure_collection(self) -> None:
        """Create the collection/table/index if it doesn't exist."""

    @abstractmethod
    async def upsert(
        self,
        ids: list[str],
        vectors: list[list[float]],
        texts: list[str],
        metadatas: list[dict],
    ) -> None:
        """Insert or update vectors."""

    @abstractmethod
    async def query(
        self,
        vector: list[float],
        top_k: int = 5,
        filter: dict | None = None,
    ) -> list[dict]:
        """Return the top-k most similar results.

        Each dict: {"id": str, "score": float, "text": str, "metadata": dict}

        filter — optional key/value pairs matched against stored metadata
                 (e.g. {"job_id": "..."}) to scope the search.
        """
