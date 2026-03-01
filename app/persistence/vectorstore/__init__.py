from app.persistence.vectorstore.base import VectorStore
from app.persistence.vectorstore.factory import get_vector_store

# __all__ public api declaration.
# only these two instances will be exported when from app.persistence.vectorstore import *
# It’s Basically Public API Declaration

__all__ = ["VectorStore", "get_vector_store"]

