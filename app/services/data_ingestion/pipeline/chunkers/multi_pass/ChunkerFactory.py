"""Factory that provides the right multi-pass chunker based on document profile."""

from app.services.data_ingestion.pipeline.chunkers.multi_pass.Base import BaseChunker
from app.services.data_ingestion.pipeline.chunkers.multi_pass.CvChunker import CvChunker

# Map document profile → chunker class
# CvChunker handles all profiles internally via its profile system,
# but the registry allows swapping in specialised chunker classes later.
_CHUNKER_REGISTRY: dict[str, type[BaseChunker]] = {
    "cv": CvChunker,
    "book": CvChunker,
    "invoice": CvChunker,
    "report": CvChunker,
    "legal": CvChunker,
    "generic": CvChunker,
    "auto": CvChunker,
}


def get_chunker(profile: str = "auto") -> BaseChunker:
    """Return a chunker instance configured for the given profile.

    Args:
        profile: Document profile — "cv", "book", "invoice", "report",
                 "legal", "generic", or "auto" (auto-detect).
    """
    chunker_cls = _CHUNKER_REGISTRY.get(profile.lower(), CvChunker)
    return chunker_cls(profile=profile)
