#!/usr/bin/env python3
"""
Test the S3 Bytes Store using the StoreProviderS3
"""

import uuid

from langchain_core.stores import BaseStore

from srai_store.store_provider_s3 import StoreProviderS3


def clear_store(test_store: BaseStore[str, bytes]) -> None:
    """Clear all test data from the store."""
    print("Clearing test store...")
    ids = []
    for key in test_store.yield_keys():
        ids.append(key)
    if ids:
        test_store.mdelete(ids)
        print(f"Cleared {len(ids)} keys")
    else:
        print("Store is already empty")


def test_set_and_get(test_store: BaseStore[str, bytes]) -> None:
    """Test basic set and get operations."""
    print("\n=== Test: Set and Get ===")
    test_id = f"test_{uuid.uuid4()}"
    test_data = b"Hello, S3 World!"

    # Set data
    print(f"Setting data for key: {test_id}")
    test_store.mset([(test_id, test_data)])

    # Get data
    print(f"Getting data for key: {test_id}")
    retrieved_data = test_store.mget([test_id])[0]

    if retrieved_data is None:
        raise Exception("Data not found after set operation")
    if retrieved_data != test_data:
        raise Exception(f"Data mismatch. Expected: {test_data}, Got: {retrieved_data}")

    print("✓ Set and Get test passed")

    # Cleanup
    test_store.mdelete([test_id])


def test_get_nonexistent(test_store: BaseStore[str, bytes]) -> None:
    """Test getting a non-existent key."""
    print("\n=== Test: Get Non-existent Key ===")
    nonexistent_id = f"nonexistent_{uuid.uuid4()}"

    result = test_store.mget([nonexistent_id])[0]

    if result is not None:
        raise Exception(f"Expected None for non-existent key, got: {result}")

    print("✓ Get non-existent key test passed")


def test_delete(test_store: BaseStore[str, bytes]) -> None:
    """Test delete operation."""
    print("\n=== Test: Delete ===")
    test_id = f"test_{uuid.uuid4()}"
    test_data = b"Data to be deleted"

    # Set data
    test_store.mset([(test_id, test_data)])

    # Verify it exists
    if test_store.mget([test_id])[0] is None:
        raise Exception("Data not found after set operation")

    # Delete data
    print(f"Deleting data for key: {test_id}")
    test_store.mdelete([test_id])

    # Verify it's deleted
    result = test_store.mget([test_id])[0]
    if result is not None:
        raise Exception(f"Data still exists after delete: {result}")

    print("✓ Delete test passed")


def test_mset_and_mget(test_store: BaseStore[str, bytes]) -> None:
    """Test batch set and get operations."""
    print("\n=== Test: Batch Set and Get ===")
    test_ids = [f"test_{uuid.uuid4()}" for _ in range(3)]
    test_data_list = [
        b"First batch item",
        b"Second batch item",
        b"Third batch item",
    ]

    # Batch set
    print(f"Batch setting {len(test_ids)} items")
    key_value_pairs = list(zip(test_ids, test_data_list))
    test_store.mset(key_value_pairs)

    # Batch get
    print(f"Batch getting {len(test_ids)} items")
    retrieved_data = test_store.mget(test_ids)

    for i, (expected, actual) in enumerate(zip(test_data_list, retrieved_data)):
        if actual is None:
            raise Exception(f"Item {i} not found after batch set")
        if actual != expected:
            raise Exception(f"Item {i} data mismatch. Expected: {expected}, Got: {actual}")

    print("✓ Batch set and get test passed")

    # Cleanup
    test_store.mdelete(test_ids)


def test_mdelete(test_store: BaseStore[str, bytes]) -> None:
    """Test batch delete operation."""
    print("\n=== Test: Batch Delete ===")
    test_ids = [f"test_{uuid.uuid4()}" for _ in range(3)]
    test_data_list = [
        b"First item to delete",
        b"Second item to delete",
        b"Third item to delete",
    ]

    # Batch set
    key_value_pairs = list(zip(test_ids, test_data_list))
    test_store.mset(key_value_pairs)

    # Verify they exist
    retrieved = test_store.mget(test_ids)
    if any(item is None for item in retrieved):
        raise Exception("Not all items found after batch set")

    # Batch delete
    print(f"Batch deleting {len(test_ids)} items")
    test_store.mdelete(test_ids)

    # Verify they're deleted
    retrieved = test_store.mget(test_ids)
    if any(item is not None for item in retrieved):
        raise Exception("Some items still exist after batch delete")

    print("✓ Batch delete test passed")


def test_yield_keys(test_store: BaseStore[str, bytes]) -> None:
    """Test yielding keys."""
    print("\n=== Test: Yield Keys ===")
    test_prefix = f"yield_test_{uuid.uuid4().hex[:8]}"
    test_ids = [f"{test_prefix}_{i}" for i in range(3)]
    test_data = b"Yield test data"

    # Set data with common prefix
    key_value_pairs = [(test_id, test_data) for test_id in test_ids]
    test_store.mset(key_value_pairs)

    # Yield all keys
    print("Yielding all keys")
    all_keys = list(test_store.yield_keys())
    for test_id in test_ids:
        if test_id not in all_keys:
            raise Exception(f"ID {test_id} not found in yield_keys()")

    # Yield keys with prefix
    print(f"Yielding keys with prefix: {test_prefix}")
    prefixed_keys = list(test_store.yield_keys(prefix=test_prefix))
    if len(prefixed_keys) != len(test_ids):
        raise Exception(f"Expected {len(test_ids)} keys with prefix, got {len(prefixed_keys)}")
    for test_id in test_ids:
        if test_id not in prefixed_keys:
            raise Exception(f"ID {test_id} not found in yield_keys with prefix")

    print("✓ Yield keys test passed")

    # Cleanup
    test_store.mdelete(test_ids)


def test_binary_data(test_store: BaseStore[str, bytes]) -> None:
    """Test storing and retrieving binary data."""
    print("\n=== Test: Binary Data ===")
    test_id = f"binary_test_{uuid.uuid4()}"

    # Create some binary data (simulating an image or other binary file)
    binary_data = bytes(range(256))

    # Set binary data
    print(f"Setting binary data for key: {test_id}")
    test_store.mset([(test_id, binary_data)])

    # Get binary data
    print(f"Getting binary data for key: {test_id}")
    retrieved_data = test_store.mget([test_id])[0]

    if retrieved_data is None:
        raise Exception("Binary data not found after set operation")
    if retrieved_data != binary_data:
        raise Exception(f"Binary data mismatch. Expected length: {len(binary_data)}, " f"Got length: {len(retrieved_data)}")

    print("✓ Binary data test passed")

    # Cleanup
    test_store.mdelete([test_id])


def test_large_data(test_store: BaseStore[str, bytes]) -> None:
    """Test storing and retrieving large data."""
    print("\n=== Test: Large Data ===")
    test_id = f"large_test_{uuid.uuid4()}"

    # Create large data (1 MB)
    large_data = b"X" * (1024 * 1024)

    # Set large data
    print(f"Setting large data (1 MB) for key: {test_id}")
    test_store.mset([(test_id, large_data)])

    # Get large data
    print(f"Getting large data for key: {test_id}")
    retrieved_data = test_store.mget([test_id])[0]

    if retrieved_data is None:
        raise Exception("Large data not found after set operation")
    if len(retrieved_data) != len(large_data):
        raise Exception(f"Large data size mismatch. Expected: {len(large_data)}, " f"Got: {len(retrieved_data)}")

    print("✓ Large data test passed")

    # Cleanup
    test_store.mdelete([test_id])


def run_all_tests() -> None:
    """Run all test cases."""
    print("=" * 60)
    print("S3 Bytes Store Test Suite (using StoreProviderS3)")
    print("=" * 60)
    import os

    S3_BUCKET_CONNECTION_STRING_CACHE = os.environ.get("S3_BUCKET_CONNECTION_STRING_CACHE")
    if S3_BUCKET_CONNECTION_STRING_CACHE is None:
        print("\n⚠ S3_BUCKET_CONNECTION_STRING_CACHE environment variable not set.")
        print("Set it in format: aws_access_key;aws_secret_key;region;bucket_name")
        print("Tests skipped.")
        return

    print("\nInitializing S3 provider with connection string...")
    import sys

    sys.stdout.flush()

    try:
        html_cache_store_provider = StoreProviderS3(S3_BUCKET_CONNECTION_STRING_CACHE)
        print("Provider created, getting bytes store...")
        sys.stdout.flush()
        test_store = html_cache_store_provider.get_bytes_store("test_bytes_store")
        print("Bytes store obtained")
        sys.stdout.flush()
        assert test_store is not None
    except Exception as e:
        print(f"\n⚠ Failed to initialize S3 store: {e}")
        print("Please check your S3 bucket exists and credentials are correct.")
        print("Tests skipped.")
        import traceback

        traceback.print_exc()
        return

    try:
        # Clean up before tests
        clear_store(test_store)

        # Run all tests
        test_set_and_get(test_store)
        test_get_nonexistent(test_store)
        test_delete(test_store)
        test_mset_and_mget(test_store)
        test_mdelete(test_store)
        test_yield_keys(test_store)
        test_binary_data(test_store)
        test_large_data(test_store)

        # Clean up after tests
        clear_store(test_store)

        print("\n" + "=" * 60)
        print("✓ All tests passed successfully!")
        print("=" * 60)

    except Exception as e:
        print("\n" + "=" * 60)
        print(f"✗ Test failed: {e!s}")
        print("=" * 60)
        raise


if __name__ == "__main__":
    run_all_tests()
