#!/usr/bin/env python3
"""
Test the whole server"""

import asyncio
import json
import uuid

from langchain_core.stores import BaseStore

from srai_store.store_provider_mongo import StoreProviderMongo


async def clear_store(test_store: BaseStore[str, dict]):
    print("Clearing store")
    ids = []
    for key in test_store.yield_keys():
        ids.append(key)
    test_store.mdelete(ids)
    print(f"Cleared {len(ids)} keys")


async def test_dict_store_mongo(test_store: BaseStore[str, dict]):
    document_id = str(uuid.uuid4())
    # test insert
    document = {"brand_name": "Invest in Bansko", "brand_url": document_id}
    print(f"Inserting document: {json.dumps(document, indent=4)}")
    test_store.mset([(document_id, document)])
    # test get
    print(f"Getting document: {json.dumps(test_store.mget([document_id]), indent=4)}")
    if test_store.mget([document_id])[0] is None:
        raise Exception("Document not found")
    # test yield keys
    keys = []
    print("Yielding keys")
    for key in test_store.yield_keys():
        print(key)
        keys.append(key)
    if document_id not in keys:
        raise Exception("Document not found in yield keys")

    # test delete
    print(f"Deleting document: {document_id}")
    test_store.mdelete([document_id])
    if test_store.mget([document_id])[0] is not None:
        raise Exception("Document not deleted")


if __name__ == "__main__":
    store_provider = StoreProviderMongo("mongodb://localhost:27017", initialize=True)
    test_store = store_provider.get_dict_store("test_store")
    asyncio.run(clear_store(test_store))
    asyncio.run(test_dict_store_mongo(test_store))
    asyncio.run(clear_store(test_store))
