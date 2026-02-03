import hashlib
from abc import abstractmethod
from typing import List, Optional

from langchain_core.embeddings import Embeddings

from srai_store.dict_store_base import DictStoreBase


class EmbeddingModelBase(Embeddings):
    def __init__(
        self,
        embedding_model_name: str,
        embedding_dimension: int,
        cache_store: Optional[DictStoreBase] = None,
    ):
        self.embedding_model_name = embedding_model_name
        self.embedding_dimension = embedding_dimension
        self.cache_store = cache_store

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        # Efficient bulk implementation using cache mget, batch embedding and cache update
        results = []
        if self.cache_store is None:
            # No cache, just compute everything and return
            return self._embed_documents(texts)

        # Compute cache keys for all texts
        cache_keys = [hashlib.sha256((self.embedding_model_name + text).encode()).hexdigest() for text in texts]

        # Grab all cache results at once
        cached_dicts = self.cache_store.mget(cache_keys)

        # Determine which are missing
        missing_cache_keys = []
        missing_texts = []
        results_dict = {}
        for text, cache_key, cache_dict in zip(texts, cache_keys, cached_dicts):
            if cache_dict is None:
                missing_cache_keys.append(cache_key)
                missing_texts.append(text)
            else:
                results_dict[cache_key] = cache_dict["embedding_list"]

        # Embed only missing texts in one batch call
        if len(missing_cache_keys) > 0:
            missing_embeddings = self._embed_documents(missing_texts)
            missing_entries = [
                (cache_key, {"embedding_list": embedding}) for cache_key, embedding in zip(missing_cache_keys, missing_embeddings)
            ]
            self.cache_store.mset(missing_entries)
            for cache_key, embedding in zip(missing_cache_keys, missing_embeddings):
                results_dict[cache_key] = embedding
        results = [results_dict[cache_key] for cache_key in cache_keys]
        return results

    def embed_query(self, text: str) -> List[float]:
        if self.cache_store is None:
            return self._embed_query(text)
        cache_key = hashlib.sha256((self.embedding_model_name + text).encode()).hexdigest()
        cache_dict = self.cache_store.get(cache_key)
        if cache_dict is not None:
            return cache_dict["embedding_list"]
        embedding = self._embed_query(text)
        self.cache_store.set(cache_key, {"embedding_list": embedding})
        return embedding

    @abstractmethod
    def _embed_query(self, string: str) -> List[float]:
        raise NotImplementedError()

    @abstractmethod
    def _embed_documents(self, texts: list[str]) -> List[List[float]]:
        raise NotImplementedError()

    @staticmethod
    def get_embedding_dimension(embedding_model_name: str) -> int:
        if embedding_model_name == "openai-text-embedding-3-small":
            return 1536
        elif embedding_model_name == "openai-text-embedding-3-large":
            return 3072
        else:
            raise ValueError(f"Unknown embedding model name: {embedding_model_name}")
