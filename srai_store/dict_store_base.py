from abc import abstractmethod
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from langchain_core.stores import BaseStore

from srai_store.exceptions import KeyNotFoundError


class DictStoreBase(BaseStore[str, dict]):
    def __init__(self, collection_name: str) -> None:
        self.collection_name = collection_name

    @abstractmethod
    def mset(self, key_value_pairs: Sequence[tuple[str, dict]]) -> None:
        pass

    def set(self, key: str, value: dict) -> None:
        self.mset([(key, value)])

    @abstractmethod
    def mget(self, keys: Sequence[str]) -> List[Optional[dict]]:
        pass

    def get_raise(self, key: str) -> dict:
        dict = self.mget([key])[0]
        if dict is None:
            raise KeyNotFoundError(key)
        return dict

    def get(self, key: str) -> Optional[dict]:
        return self.mget([key])[0]

    def count(self) -> int:
        return len(list(self.yield_keys()))

    @abstractmethod
    def mdelete(self, keys: Sequence[str]) -> None:
        pass

    @abstractmethod
    def yield_keys(self, *, prefix: Optional[str] = None) -> Iterator[str]:
        pass

    @abstractmethod
    async def asample(self, count: int) -> List[dict]:
        pass

    @abstractmethod
    def query(
        self,
        query: Dict[str, Any],
        order_by: Optional[List[Tuple[str, bool]]] = None,
        limit: int = 0,
        offset: int = 0,
    ) -> List[dict]:
        pass

    @abstractmethod
    def count_query(
        self,
        query: Dict[str, Any],
    ) -> int:
        pass
