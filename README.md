# srai-store

## install
pip install srai-store

## Changelog

### 0.1.6

- **DuckDB backend**: `BytesStoreDuckdb`, `DictStoreDuckdb`, `StoreProviderDuckdb`, and `VectorStoreProviderDuckdb` (collection DB files use `.duckdb` under your data directory).
- **DuckDB queries**: comparisons use `json_extract_string` / numeric casts so filtering on plain strings (e.g. `brand_name`) does not hit DuckDB’s “Malformed JSON” error when binding Python strings.
- **Dependency**: `duckdb`.

### 0.1.5 and earlier

- **SQLite `DictStoreSqlite`**: query documents with SQLite `json_extract`; Mongo-style operators (`$eq`, `$ne`, `$lt`, `$lte`, `$gt`, `$gte`, `$in`); `count_query`; fixes for `mget` / `mdelete` with multiple keys and JSON parsing for reads.
- **Mongo `DictStoreMongo`**: query helpers aligned with the same operator-style filters (mapped to `document.*` fields).
- **`ObjectStoreNested`**: `count_query` delegates to the underlying dict store; query types widened to `Dict[str, Any]`.
- **`commit_and_publish.py`**: optional release script (version bump, commit, tag, push, PyPI publish; skips when there are no changes; PyPI token from env).


### Dev notes

python commit_and_publish.py to move to pipy