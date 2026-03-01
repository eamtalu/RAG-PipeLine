"""Qdrant-backed vector store."""

from qdrant_client import AsyncQdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue

from app.config import settings
from app.persistence.vectorstore.base import VectorStore


class QdrantVectorStore(VectorStore):
    def __init__(self) -> None:
        self.client = AsyncQdrantClient(url=settings.qdrant_url)
        self.collection = settings.qdrant_collection

    async def ensure_collection(self) -> None:
        collections = await self.client.get_collections()
        names = [c.name for c in collections.collections]
        if self.collection not in names:
            await self.client.create_collection(
                collection_name=self.collection,
                vectors_config=VectorParams(
                    size=settings.embedding_dimensions,
                    distance=Distance.COSINE,
                ),
            )

    async def upsert(
        self,
        ids: list[str],
        vectors: list[list[float]],
        texts: list[str],
        metadatas: list[dict],
    ) -> None:
        points = [
            PointStruct(
                id=id_,
                vector=vec,
                payload={"text": txt, **meta},
            )
            for id_, vec, txt, meta in zip(ids, vectors, texts, metadatas)
        ]
        await self.client.upsert(collection_name=self.collection, points=points)

    async def query(self, vector: list[float], top_k: int = 5, filter: dict | None = None) -> list[dict]:
        query_filter = None
        if filter:
            query_filter = Filter(
                must=[
                    FieldCondition(key=k, match=MatchValue(value=v))
                    for k, v in filter.items()
                ]
            )

        results = await self.client.query_points(
            collection_name=self.collection,
            query=vector,
            limit=top_k,
            query_filter=query_filter,
        )
        return [
            {
                "id": str(hit.id),
                "score": hit.score,
                "text": hit.payload.get("text", ""),
                "metadata": {k: v for k, v in hit.payload.items() if k != "text"},
            }
            for hit in results.points
        ]
