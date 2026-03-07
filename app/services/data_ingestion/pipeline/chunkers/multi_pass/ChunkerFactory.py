from app.services.data_ingestion.pipeline.chunkers.multi_pass.Base import BaseChunker
from app.services.data_ingestion.pipeline.chunkers.multi_pass.CvChunker import CvChunker

#Map the Document Profile to its chunker class
_CHUNKER_REGISTRY : dict[str,type[BaseChunker]] = {
    "CV":CvChunker
}

def get_chunker() -> BaseChunker:
    chunker_classname = _CHUNKER_REGISTRY.get("CV")
    if chunker_classname is None:
        raise Exception(f"Chunker class {_CHUNKER_REGISTRY.get('CV')} not found")

    return chunker_classname()