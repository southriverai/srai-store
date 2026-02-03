import json
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union

from srai_store.dict_store_base import DictStoreBase


class DictStoreSqlite(DictStoreBase):
    def __init__(self, collection_name: str, path_file_database: Path) -> None:
        super().__init__(collection_name)
        self.path_file_database = path_file_database
        # Ensure parent directory exists
        abs_path = self.path_file_database.absolute()
        parent_dir = abs_path.parent
        if parent_dir:
            parent_dir.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the SQLite database."""
        with self._get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS store (
                    key TEXT PRIMARY KEY,
                    document JSON
                )
            """
            )
            conn.commit()

    @contextmanager
    def _get_connection(self):
        """Get a database connection with proper cleanup."""
        conn = sqlite3.connect(self.path_file_database)
        try:
            yield conn
        finally:
            conn.close()

    def _validate_key(self, key: str) -> None:
        """Validate the key to ensure it has valid characters."""
        if not re.match(r"^[a-zA-Z0-9_.\-/]+$", key):
            raise ValueError(f"Invalid characters in key: {key}")

    def _validate_query_keys(self, query: Dict[str, Any]) -> None:
        """Validate query keys (field paths)."""
        for key in query:
            if not re.match(r"^[a-zA-Z0-9_.\-/]+$", key):
                raise ValueError(f"Invalid characters in query key: {key}")

    # MongoDB-style query operators
    _QUERY_OPS = frozenset(("$eq", "$ne", "$lt", "$lte", "$gt", "$gte", "$in"))

    def mset(self, key_value_pairs: Sequence[tuple[str, dict]]) -> None:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            for key, document in key_value_pairs:
                self._validate_key(key)
                cursor.execute(
                    "REPLACE INTO store (key, document) VALUES (?, ?)",
                    (key, json.dumps(document)),
                )
            conn.commit()

    def mget(self, keys: Sequence[str]) -> List[Optional[dict]]:
        """Get the values associated with the given keys.

        Args:
            keys: A sequence of keys.

        Returns:
            A sequence of optional values associated with the keys.
            If a key is not found, the corresponding value will be None.
        """
        if not keys:
            return []
        for key in keys:
            self._validate_key(key)
        placeholders = ",".join("?" * len(keys))
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT key, document FROM store WHERE key IN ({placeholders})",
                list(keys),
            )
            key_to_doc = {row[0]: json.loads(row[1]) if row[1] else None for row in cursor.fetchall()}
        return [key_to_doc.get(k) for k in keys]

    def mdelete(self, keys: Sequence[str]) -> None:
        if not keys:
            return
        for key in keys:
            self._validate_key(key)
        placeholders = ",".join("?" * len(keys))
        with self._get_connection() as conn:
            conn.execute(
                f"DELETE FROM store WHERE key IN ({placeholders})",
                list(keys),
            )
            conn.commit()

    def yield_keys(self, *, prefix: Optional[str] = None) -> Union[Iterator[str], Iterator[str]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if prefix:
                self._validate_key(prefix)
                cursor.execute("SELECT key FROM store WHERE key LIKE ?", (f"{prefix}%",))
            else:
                cursor.execute("SELECT key FROM store")
            for row in cursor:
                yield row[0]

    def clear(self) -> None:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM store")
            conn.commit()

    async def asample(self, count: int) -> List[dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT document FROM store ORDER BY RANDOM() LIMIT ?",
                (count,),
            )
            rows = cursor.fetchall()
            return [json.loads(row[0]) for row in rows if row[0]]

    def _json_path(self, field: str) -> str:
        """Convert field name to SQLite JSON path. Supports nested: 'user.name' -> $.user.name."""
        return "$." + field

    def _build_json_query(self, query: Dict[str, Any]) -> tuple[str, list[Any]]:
        """Build WHERE clause and params using json_extract. Supports MongoDB-style operators."""
        if not query:
            return "1=1", []
        self._validate_query_keys(query)
        conditions: list[str] = []
        params: list[Any] = []
        for field, value in query.items():
            path = self._json_path(field)
            if isinstance(value, dict) and value:
                op = next(iter(value))
                if op not in self._QUERY_OPS:
                    raise ValueError(f"Unknown query operator: {op}. Use one of {self._QUERY_OPS}")
                op_value = value[op]
                if op == "$eq":
                    conditions.append("json_extract(document, ?) = ?")
                    params.extend([path, op_value])
                elif op == "$ne":
                    conditions.append("(json_extract(document, ?) IS NULL OR json_extract(document, ?) != ?)")
                    params.extend([path, path, op_value])
                elif op == "$lt":
                    conditions.append("json_extract(document, ?) < ?")
                    params.extend([path, op_value])
                elif op == "$lte":
                    conditions.append("json_extract(document, ?) <= ?")
                    params.extend([path, op_value])
                elif op == "$gt":
                    conditions.append("json_extract(document, ?) > ?")
                    params.extend([path, op_value])
                elif op == "$gte":
                    conditions.append("json_extract(document, ?) >= ?")
                    params.extend([path, op_value])
                elif op == "$in":
                    if not isinstance(op_value, (list, tuple)):
                        raise ValueError("$in requires a list or tuple")
                    placeholders = ",".join("?" * len(op_value))
                    conditions.append(f"json_extract(document, ?) IN ({placeholders})")
                    params.extend([path, *op_value])
            else:
                conditions.append("json_extract(document, ?) = ?")
                params.extend([path, value])
        return " AND ".join(conditions), params

    def _build_order_by(self, order_by: List[Tuple[str, bool]]) -> str:
        """Build ORDER BY clause for JSON fields."""
        if not order_by:
            return ""
        parts = []
        for field, ascending in order_by:
            self._validate_key(field)
            path = self._json_path(field)
            direction = "ASC" if ascending else "DESC"
            parts.append(f"json_extract(document, '{path}') {direction}")
        return " ORDER BY " + ", ".join(parts)

    def query(
        self,
        query: Dict[str, Any],
        order_by: Optional[List[Tuple[str, bool]]] = None,
        limit: int = 0,
        offset: int = 0,
    ) -> List[dict]:
        """Query the database by matching fields inside the JSON documents.

        Uses SQLite's json_extract() to query inside the document. Supports nested
        paths via dotted keys, e.g. {"user.name": "Alice"} matches $.user.name.

        MongoDB-style operators (field value as dict):
            $eq, $ne, $lt, $lte, $gt, $gte, $in

        Args:
            query: Field -> value or field -> {operator: value}. All conditions ANDed.
            order_by: List of (field, ascending).
            limit: Max results (0 = no limit).
            offset: Number to skip.

        Returns:
            A list of matching dictionaries.
        """
        where_clause, params = self._build_json_query(query)
        order_clause = self._build_order_by(order_by or [])
        limit_clause = f" LIMIT {limit}" if limit > 0 else ""
        offset_clause = f" OFFSET {offset}" if offset > 0 else ""

        sql = f"SELECT document FROM store WHERE {where_clause}{order_clause}{limit_clause}{offset_clause}"
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            return [json.loads(row[0]) for row in rows if row[0]]

    def count_query(
        self,
        query: Dict[str, Any],
    ) -> int:
        """Count documents matching the query."""
        where_clause, params = self._build_json_query(query)
        sql = f"SELECT COUNT(*) FROM store WHERE {where_clause}"
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            row = cursor.fetchone()
            return row[0] if row else 0
