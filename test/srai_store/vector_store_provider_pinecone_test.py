import os

# Disable LangSmith tracing
os.environ["LANGCHAIN_TRACING_V2"] = "false"
os.environ["LANGCHAIN_ENDPOINT"] = ""
os.environ["LANGCHAIN_API_KEY"] = ""

from srai_store.embedding_model_base import EmbeddingModelBase
from srai_store.vector_store_provider_pinecone import VectorStoreProviderPinecone


def test_vector_store_provider_pinecone():
    vector_store_provider = VectorStoreProviderPinecone(database_name="test", api_key="test", cloud="aws", region="us-east-1")
    vector_store = vector_store_provider.get_vector_store("test", embeddings_model=EmbeddingModelBase(name="test", dimension=128))
    vector_store.add_texts(["Hello, world!"])
    vector_store.add_texts(["Hello, world! 2"])
    result = vector_store.similarity_search_with_score("Hello, world!")
    print(result)
    result = vector_store.similarity_search_with_score("Hello, world! 2")
    print(result)
    # dont delete the vector store, pinecone is slow af
    # vector_store.delete(delete_all=True)
    # print("Vector store deleted")


if __name__ == "__main__":
    test_vector_store_provider_pinecone()
