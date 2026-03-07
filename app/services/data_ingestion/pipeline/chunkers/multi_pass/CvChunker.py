
from app.services.data_ingestion.pipeline.chunkers.multi_pass.Base import BaseChunker


class CvChunker(BaseChunker):
    def chunk(self, doc_profile : str, text : str) -> str:
        pass