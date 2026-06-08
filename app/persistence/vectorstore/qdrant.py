"""Qdrant-backed vector store with hybrid search (vector + text match)."""

from qdrant_client import AsyncQdrantClient
from qdrant_client.models import (
    Distance, VectorParams, PointStruct,
    Filter, FieldCondition, MatchValue, MatchText,
    PayloadSchemaType, TextIndexParams, TokenizerType,
)

from app.settings import settings
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

        # KEYWORD indexes — exact match filtering
        for field, schema_type in [
            ("profile", PayloadSchemaType.KEYWORD),
            ("section_root", PayloadSchemaType.KEYWORD),
            ("section_parent", PayloadSchemaType.KEYWORD),
            ("section_heading", PayloadSchemaType.KEYWORD),
            ("job_id", PayloadSchemaType.KEYWORD),
            ("chunk_type", PayloadSchemaType.KEYWORD),
            ("parent_id", PayloadSchemaType.KEYWORD),
            ("document_type", PayloadSchemaType.KEYWORD),
        ]:
            await self.client.create_payload_index(
                collection_name=self.collection,
                field_name=field,
                field_schema=schema_type,
            )

        # TEXT index — full-text substring matching for hybrid search
        await self.client.create_payload_index(
            collection_name=self.collection,
            field_name="text",
            field_schema=TextIndexParams(
                type="text",
                tokenizer=TokenizerType.WORD,
                min_token_len=2,
                max_token_len=20,
                lowercase=True,
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

    async def query(
        self,
        vector: list[float],
        top_k: int = 5,
        filter: dict | None = None,
        text_match: dict | None = None,
    ) -> list[dict]:
        conditions: list[FieldCondition] = []

        # Exact match conditions (KEYWORD-indexed fields)
        if filter:
            conditions.extend(
                FieldCondition(key=k, match=MatchValue(value=v))
                for k, v in filter.items()
            )

        # Full-text match conditions (TEXT-indexed fields)
        if text_match:
            conditions.extend(
                FieldCondition(key=k, match=MatchText(text=v))
                for k, v in text_match.items()
            )

        query_filter = Filter(must=conditions) if conditions else None

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
