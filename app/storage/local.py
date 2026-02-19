import aiofiles
from pathlib import Path

from app.storage.base import ObjectStorage


class LocalStorage(ObjectStorage):
    """File-system storage for development. Replace with S3Storage in prod."""

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _resolve(self, key: str) -> Path:
        return self.base_dir / key

    async def save(self, key: str, data: bytes) -> str:
        path = self._resolve(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(path, "wb") as f:
            await f.write(data)
        return str(path)

    async def load(self, key: str) -> bytes:
        async with aiofiles.open(self._resolve(key), "rb") as f:
            return await f.read()

    async def delete(self, key: str) -> None:
        path = self._resolve(key)
        if path.exists():
            path.unlink()

    async def exists(self, key: str) -> bool:
        return self._resolve(key).exists()
