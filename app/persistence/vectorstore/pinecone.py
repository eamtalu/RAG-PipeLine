"""Pinecone-backed vector store."""

from pinecone import Pinecone

from app.config import settings
from app.persistence.vectorstore.base import VectorStore


class PineconeVectorStore(VectorStore):
    def __init__(self) -> None:
        self.pc = Pinecone(api_key=settings.pinecone_api_key)
        self.index_name = settings.pinecone_index
        self._index = None

    def _get_index(self):
        if self._index is None:
            self._index = self.pc.Index(self.index_name)
        return self._index

    async def ensure_collection(self) -> None:
        existing = [idx.name for idx in self.pc.list_indexes()]
        if self.index_name not in existing:
            self.pc.create_index(
                name=self.index_name,
                dimension=settings.embedding_dimensions,
                metric="cosine",
                spec={"serverless": {"cloud": "aws", "region": "us-east-1"}},
            )

    async def upsert(
        self,
        ids: list[str],
        vectors: list[list[float]],
        texts: list[str],
        metadatas: list[dict],
    ) -> None:
        index = self._get_index()
        records = [
            {"id": id_, "values": vec, "metadata": {"text": txt, **meta}}
            for id_, vec, txt, meta in zip(ids, vectors, texts, metadatas)
        ]
        index.upsert(vectors=records)

    async def query(self, vector: list[float], top_k: int = 5, filter: dict | None = None) -> list[dict]:
        index = self._get_index()
        pc_filter = {k: {"$eq": v} for k, v in filter.items()} if filter else None
        result = index.query(vector=vector, top_k=top_k, include_metadata=True, filter=pc_filter)
        return [
            {
                "id": match.id,
                "score": match.score,
                "text": match.metadata.get("text", ""),
                "metadata": {k: v for k, v in match.metadata.items() if k != "text"},
            }
            for match in result.matches
        ]
