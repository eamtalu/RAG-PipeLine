"""Factory to instantiate the configured vector store backend."""

from app.settings import settings
from app.persistence.vectorstore.base import VectorStore


def get_vector_store() -> VectorStore:
    match settings.vector_store_backend:
        case "pgvector":
            from app.persistence.vectorstore.pgvector import PgVectorStore
            return PgVectorStore()
        case "qdrant":
            from app.persistence.vectorstore.qdrant import QdrantVectorStore
            return QdrantVectorStore()
        case "pinecone":
            from app.persistence.vectorstore.pinecone import PineconeVectorStore
            return PineconeVectorStore()
        case other:
            raise ValueError(f"Unknown vector_store_backend: {other}")
