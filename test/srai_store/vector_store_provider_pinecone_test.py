import os

# Disable LangSmith tracing
os.environ["LANGCHAIN_TRACING_V2"] = "false"
os.environ["LANGCHAIN_ENDPOINT"] = ""
os.environ["LANGCHAIN_API_KEY"] = ""

from mailau_server.mailau_container import MailauContainer


def test_vector_store_provider_pinecone():
    mailau_container = MailauContainer.initialize()
    vector_store = mailau_container.vector_store_provider.get_vector_store("test")
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
