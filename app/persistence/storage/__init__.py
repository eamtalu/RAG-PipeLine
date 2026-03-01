from app.settings import settings
from app.persistence.storage.base import ObjectStorage
from app.persistence.storage.local import LocalStorage

__all__ = ["ObjectStorage", "LocalStorage", "get_storage"]


def get_storage() -> ObjectStorage:
    """FastAPI dependency — provides the storage bean."""
    return LocalStorage(settings.upload_dir)
