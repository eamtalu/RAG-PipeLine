"""pgvector-backed vector store (default)."""

from sqlalchemy import text

from app.config import settings
from app.models.database import engine
from app.vectorstore.base import VectorStore


class PgVectorStore(VectorStore):
    TABLE = "embeddings"

    async def ensure_collection(self) -> None:
        async with engine.begin() as conn:
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            await conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {self.TABLE} (
                    id   TEXT PRIMARY KEY,
                    embedding vector({settings.embedding_dimensions}),
                    text TEXT,
                    metadata JSONB DEFAULT '{{}}'
                )
            """))

    async def upsert(
        self,
        ids: list[str],
        vectors: list[list[float]],
        texts: list[str],
        metadatas: list[dict],
    ) -> None:
        async with engine.begin() as conn:
            for id_, vec, txt, meta in zip(ids, vectors, texts, metadatas):
                vec_str = "[" + ",".join(str(v) for v in vec) + "]"
                await conn.execute(
                    text(f"""
                        INSERT INTO {self.TABLE} (id, embedding, text, metadata)
                        VALUES (:id, :embedding, :text, :metadata::jsonb)
                        ON CONFLICT (id) DO UPDATE
                        SET embedding = EXCLUDED.embedding,
                            text = EXCLUDED.text,
                            metadata = EXCLUDED.metadata
                    """),
                    {"id": id_, "embedding": vec_str, "text": txt, "metadata": str(meta).replace("'", '"')},
                )

    async def query(self, vector: list[float], top_k: int = 5) -> list[dict]:
        vec_str = "[" + ",".join(str(v) for v in vector) + "]"
        async with engine.begin() as conn:
            result = await conn.execute(
                text(f"""
                    SELECT id, text, metadata,
                           1 - (embedding <=> :vec) AS score
                    FROM {self.TABLE}
                    ORDER BY embedding <=> :vec
                    LIMIT :k
                """),
                {"vec": vec_str, "k": top_k},
            )
            return [
                {"id": row.id, "score": float(row.score), "text": row.text, "metadata": row.metadata}
                for row in result.fetchall()
            ]
