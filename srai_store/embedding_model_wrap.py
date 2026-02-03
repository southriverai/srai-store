from typing import List, Optional

from langchain_core.embeddings import Embeddings

from srai_store.dict_store_base import DictStoreBase
from srai_store.embedding_model_base import EmbeddingModelBase


class EmbeddingModelWrap(EmbeddingModelBase):
    def __init__(
        self,
        embedding_model_name: str,
        embedding_model: Embeddings,
        cache_store: Optional[DictStoreBase] = None,
    ):
        embedding_dimension = EmbeddingModelBase.get_embedding_dimension(embedding_model_name)
        super().__init__(embedding_model_name, embedding_dimension, cache_store)
        self.embedding_model = embedding_model

    def _embed_query(self, string: str) -> List[float]:
        return self.embedding_model.embed_query(string)

    def _embed_documents(self, texts: list[str]) -> List[List[float]]:
        return self.embedding_model.embed_documents(texts)
