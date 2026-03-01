from abc import ABC, abstractmethod
from pathlib import Path


class ObjectStorage(ABC):
    """Abstract interface — swap local FS for S3/GCS/MinIO later."""

    @abstractmethod
    async def save(self, key: str, data: bytes) -> str:
        """Persist *data* under *key*, return the storage path/URL."""

    @abstractmethod
    async def load(self, key: str) -> bytes:
        """Return raw bytes for *key*."""

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Remove the object at *key*."""

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Check if *key* exists."""
