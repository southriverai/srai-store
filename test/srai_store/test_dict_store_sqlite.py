#!/usr/bin/env python3
"""
Test the whole server"""

import asyncio
import json
import uuid

from srai_store.dict_store_base import DictStoreBase
from srai_store.store_provider_sqlite import StoreProviderSqlite


async def clear_store(test_store: DictStoreBase):
    print("Clearing store")
    ids = []
    for key in test_store.yield_keys():
        ids.append(key)
    test_store.mdelete(ids)
    print(f"Cleared {len(ids)} keys")


async def test_dict_store(test_store: DictStoreBase):
    document_id1 = str(uuid.uuid4())
    document_id2 = str(uuid.uuid4())
    document_id3 = str(uuid.uuid4())
    # test insert
    document1 = {"brand_name": "Invest in Bansko", "brand_url": document_id1, "size": 100}
    document2 = {"brand_name": "Invest in Bansko", "brand_url": document_id2, "size": 200}
    document3 = {"brand_name": "Invest in Bansko2", "brand_url": document_id3, "size": 300}
    print(f"Inserting document: {json.dumps(document1, indent=4)}")
    test_store.mset([(document_id1, document1)])
    # test get
    print(f"Getting document: {json.dumps(test_store.mget([document_id1]), indent=4)}")
    if test_store.mget([document_id1])[0] is None:
        raise RuntimeError("Document not found")

    # test query
    print(f"Inserting documents: {json.dumps(document2, indent=4)} and {json.dumps(document3, indent=4)}")
    test_store.mset([(document_id2, document2), (document_id3, document3)])

    query = {"brand_name": "Invest in Bansko"}
    print(f"Querying documents: {json.dumps(query, indent=4)}")
    query_result = test_store.query(query)
    print(f"Query result: {json.dumps(query_result, indent=4)}")
    if len(query_result) == 0:
        raise RuntimeError("Document not found")
    if len(query_result) != 2:
        raise RuntimeError("Incorrect number of documents found")

    query = {"brand_name": "Invest in Bansko2"}
    print(f"Querying documents: {json.dumps(query, indent=4)}")
    query_result = test_store.query(query)
    print(f"Query result: {json.dumps(query_result, indent=4)}")
    if len(query_result) == 0:
        raise RuntimeError("Document not found")
    if len(query_result) != 1:
        raise RuntimeError("Incorrect number of documents found")

    # test query with smaller or equal to (MongoDB-style $lte)
    query = {"size": {"$lte": 250}}
    print(f"Querying documents: {json.dumps(query, indent=4)}")
    query_result = test_store.query(query)
    print(f"Query result: {json.dumps(query_result, indent=4)}")
    if len(query_result) == 0:
        raise RuntimeError("Document not found")
    if len(query_result) != 2:
        raise RuntimeError("Incorrect number of documents found")  # size 100, 200

    # test query with smaller or equal to (MongoDB-style $lte)
    query = {"size": {"$lte": 100}}
    print(f"Querying documents: {json.dumps(query, indent=4)}")
    query_result = test_store.query(query)
    print(f"Query result: {json.dumps(query_result, indent=4)}")
    if len(query_result) == 0:
        raise RuntimeError("Document not found")
    if len(query_result) != 1:
        raise RuntimeError("Incorrect number of documents found")  # size 100

    # test yield keys
    keys = []
    print("Yielding keys")
    for key in test_store.yield_keys():
        print(key)
        keys.append(key)
    if document_id1 not in keys:
        raise RuntimeError("Document not found in yield keys")

    # test delete
    print(f"Deleting document: {document_id1}")
    test_store.mdelete([document_id1])
    if test_store.mget([document_id1])[0] is not None:
        raise RuntimeError("Document not deleted")


if __name__ == "__main__":
    store_provider = StoreProviderSqlite("test_store", path_dir_database="test_store.db")
    test_store = store_provider.get_dict_store("test_store")
    asyncio.run(clear_store(test_store))
    asyncio.run(test_dict_store(test_store))
    asyncio.run(clear_store(test_store))
