import hashlib
from typing import Any, Optional

from langchain.chat_models.base import BaseChatModel
from langchain_core.callbacks.manager import CallbackManagerForLLMRun
from langchain_core.messages import BaseMessage
from langchain_core.outputs import ChatResult
from pydantic import Field

from mailau_server.store.dict_store_base import DictStoreBase


class ChatModelWrapper(BaseChatModel):
    chat_model_name: Optional[str] = Field(default=None, exclude=True)
    """Name of the chat model for caching purposes."""
    chat_model_inner: Optional[BaseChatModel] = Field(default=None, exclude=True)
    """The inner chat model to wrap."""
    cache_store: Optional[DictStoreBase] = Field(default=None, exclude=True)
    """Optional cache store for caching responses."""

    def __init__(
        self,
        chat_model_name: str,
        chat_model_inner: BaseChatModel,
        cache_store: Optional[DictStoreBase] = None,
        **kwargs: Any,
    ):
        # Convert chat_model_name to a regular string to avoid serialization issues with serializable runnables
        super().__init__(**kwargs)
        # Set Pydantic fields directly
        self.chat_model_name = chat_model_name
        self.chat_model_inner = chat_model_inner
        self.cache_store = cache_store

    @property
    def _llm_type(self) -> str:
        return self.chat_model_inner._llm_type  # type: ignore

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: Optional[list[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> ChatResult:
        cache_key = ""
        if self.cache_store is not None:
            cache_key = hashlib.sha256(
                (self.chat_model_name + str(messages)).encode()  # type: ignore
            ).hexdigest()
            cache_dict = self.cache_store.get(cache_key)
            if cache_dict is not None:
                return ChatResult(**cache_dict["result"])
        result = self.chat_model_inner._generate(messages, stop, run_manager, **kwargs)  # type: ignore
        if self.cache_store is not None:
            cache_dict = {"result": result.model_dump()}
            self.cache_store.set(cache_key, cache_dict)
        return result
