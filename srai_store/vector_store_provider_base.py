from abc import ABC, abstractmethod
from typing import Optional

from mailau_server.components.embedding_model_base import EmbeddingModelBase

from srai_store.vector_store_base import VectorStoreBase


class VectorStoreProviderBase(ABC):
    def __init__(
        self,
        database_name: str,
    ):
        self.database_name = database_name

    @abstractmethod
    def get_vector_store(
        self,
        collection_name: str,
        embeddings_model: EmbeddingModelBase,
        namespace: Optional[str] = None,
    ) -> VectorStoreBase:
        raise NotImplementedError("Not implemented")
        raise NotImplementedError("Not implemented")
