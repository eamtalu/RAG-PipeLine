"""pgvector-backed vector store (default)."""

import json

from sqlalchemy import text

from app.settings import settings
from app.config.database import engine
from app.persistence.vectorstore.base import VectorStore


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
            await conn.execute(text(f"""
                CREATE INDEX IF NOT EXISTS idx_{self.TABLE}_embedding_hnsw
                ON {self.TABLE}
                USING hnsw (embedding vector_cosine_ops)
                WITH (m = 16, ef_construction = 64)
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
                    {"id": id_, "embedding": vec_str, "text": txt, "metadata": json.dumps(meta)},
                )

    async def query(self, vector: list[float], top_k: int = 5, filter: dict | None = None) -> list[dict]:
        vec_str = "[" + ",".join(str(v) for v in vector) + "]"
        params: dict = {"vec": vec_str, "k": top_k}

        where_clauses: list[str] = []
        if filter:
            for i, (key, value) in enumerate(filter.items()):
                param_name = f"fv_{i}"
                where_clauses.append(f"metadata->>:fk_{i} = :{param_name}")
                params[f"fk_{i}"] = key
                params[param_name] = str(value)

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        async with engine.begin() as conn:
            result = await conn.execute(
                text(f"""
                    SELECT id, text, metadata,
                           1 - (embedding <=> :vec) AS score
                    FROM {self.TABLE}
                    {where_sql}
                    ORDER BY embedding <=> :vec
                    LIMIT :k
                """),
                params,
            )
            return [
                {"id": row.id, "score": float(row.score), "text": row.text, "metadata": row.metadata}
                for row in result.fetchall()
            ]
