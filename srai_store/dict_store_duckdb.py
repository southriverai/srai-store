import json
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union

import duckdb

from srai_store.dict_store_base import DictStoreBase


class DictStoreDuckdb(DictStoreBase):
    def __init__(self, collection_name: str, path_file_database: Path) -> None:
        super().__init__(collection_name)
        self.path_file_database = path_file_database
        abs_path = self.path_file_database.absolute()
        parent_dir = abs_path.parent
        if parent_dir:
            parent_dir.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with self._get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS store (
                    key VARCHAR PRIMARY KEY,
                    document JSON
                )
                """
            )

    @contextmanager
    def _get_connection(self):
        conn = duckdb.connect(str(self.path_file_database))
        try:
            yield conn
        finally:
            conn.close()

    def _validate_key(self, key: str) -> None:
        if not re.match(r"^[a-zA-Z0-9_.\-/]+$", key):
            raise ValueError(f"Invalid characters in key: {key}")

    def _validate_query_keys(self, query: Dict[str, Any]) -> None:
        for key in query:
            if not re.match(r"^[a-zA-Z0-9_.\-/]+$", key):
                raise ValueError(f"Invalid characters in query key: {key}")

    _QUERY_OPS = frozenset(("$eq", "$ne", "$lt", "$lte", "$gt", "$gte", "$in"))

    @staticmethod
    def _document_from_row(document: Any) -> dict:
        if isinstance(document, dict):
            return document
        if isinstance(document, str):
            return json.loads(document)
        return json.loads(str(document))

    def mset(self, key_value_pairs: Sequence[tuple[str, dict]]) -> None:
        with self._get_connection() as conn:
            for key, document in key_value_pairs:
                self._validate_key(key)
                conn.execute(
                    "INSERT OR REPLACE INTO store (key, document) VALUES (?, ?)",
                    [key, json.dumps(document)],
                )

    def mget(self, keys: Sequence[str]) -> List[Optional[dict]]:
        if not keys:
            return []
        for key in keys:
            self._validate_key(key)
        placeholders = ",".join("?" * len(keys))
        with self._get_connection() as conn:
            rows = conn.execute(
                f"SELECT key, document FROM store WHERE key IN ({placeholders})",
                list(keys),
            ).fetchall()
            key_to_doc = {row[0]: self._document_from_row(row[1]) if row[1] is not None else None for row in rows}
        return [key_to_doc.get(k) for k in keys]

    def mdelete(self, keys: Sequence[str]) -> None:
        if not keys:
            return
        for key in keys:
            self._validate_key(key)
        placeholders = ",".join("?" * len(keys))
        with self._get_connection() as conn:
            conn.execute(f"DELETE FROM store WHERE key IN ({placeholders})", list(keys))

    def yield_keys(self, *, prefix: Optional[str] = None) -> Union[Iterator[str], Iterator[str]]:
        with self._get_connection() as conn:
            if prefix:
                self._validate_key(prefix)
                rows = conn.execute(
                    "SELECT key FROM store WHERE key LIKE ?",
                    [f"{prefix}%"],
                ).fetchall()
            else:
                rows = conn.execute("SELECT key FROM store").fetchall()
            for row in rows:
                yield row[0]

    def clear(self) -> None:
        with self._get_connection() as conn:
            conn.execute("DELETE FROM store")

    async def asample(self, count: int) -> List[dict]:
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT document FROM store ORDER BY random() LIMIT ?",
                [count],
            ).fetchall()
            return [self._document_from_row(row[0]) for row in rows if row[0] is not None]

    def _json_path(self, field: str) -> str:
        return "$." + field

    @staticmethod
    def _is_number(value: Any) -> bool:
        return isinstance(value, (int, float)) and not isinstance(value, bool)

    def _duckdb_compare_lhs(self, path: str, value: Any) -> tuple[str, list[Any]]:
        """LHS expression for comparing a JSON field to a scalar.

        DuckDB's ``json_extract`` returns JSON; comparing to a bound string makes DuckDB
        try to parse the parameter as JSON (fails for plain text). Use
        ``json_extract_string`` for strings and numeric casts for numbers.
        """
        if isinstance(value, str):
            return "json_extract_string(document, ?)", [path]
        if isinstance(value, bool):
            return "CAST(json_extract(document, ?) AS BOOLEAN)", [path]
        if self._is_number(value):
            return "try_cast(json_extract_string(document, ?) AS DOUBLE)", [path]
        return "json_extract_string(document, ?)", [path]

    def _build_json_query(self, query: Dict[str, Any]) -> tuple[str, list[Any]]:
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
                    lhs, lhs_params = self._duckdb_compare_lhs(path, op_value)
                    conditions.append(f"{lhs} = ?")
                    params.extend([*lhs_params, op_value])
                elif op == "$ne":
                    lhs, lhs_params = self._duckdb_compare_lhs(path, op_value)
                    conditions.append(f"(({lhs}) IS NULL OR ({lhs}) != ?)")
                    params.extend([*lhs_params, *lhs_params, op_value])
                elif op in ("$lt", "$lte", "$gt", "$gte"):
                    cmp_op = {"$lt": "<", "$lte": "<=", "$gt": ">", "$gte": ">="}[op]
                    lhs, lhs_params = self._duckdb_compare_lhs(path, op_value)
                    conditions.append(f"{lhs} {cmp_op} ?")
                    params.extend([*lhs_params, op_value])
                elif op == "$in":
                    if not isinstance(op_value, (list, tuple)):
                        raise ValueError("$in requires a list or tuple")
                    if not op_value:
                        conditions.append("1=0")
                        continue
                    sample = op_value[0]
                    lhs, lhs_params = self._duckdb_compare_lhs(path, sample)
                    placeholders = ",".join("?" * len(op_value))
                    conditions.append(f"{lhs} IN ({placeholders})")
                    params.extend([*lhs_params, *op_value])
            else:
                lhs, lhs_params = self._duckdb_compare_lhs(path, value)
                conditions.append(f"{lhs} = ?")
                params.extend([*lhs_params, value])
        return " AND ".join(conditions), params

    def _build_order_by(self, order_by: List[Tuple[str, bool]]) -> str:
        if not order_by:
            return ""
        parts = []
        for field, ascending in order_by:
            self._validate_key(field)
            path = self._json_path(field)
            direction = "ASC" if ascending else "DESC"
            # Prefer numeric ordering when path looks like a number field is common;
            # use string extraction for stable lexicographic order (matches text fields).
            parts.append(f"json_extract_string(document, '{path}') {direction}")
        return " ORDER BY " + ", ".join(parts)

    def query(
        self,
        query: Dict[str, Any],
        order_by: Optional[List[Tuple[str, bool]]] = None,
        limit: int = 0,
        offset: int = 0,
    ) -> List[dict]:
        where_clause, params = self._build_json_query(query)
        order_clause = self._build_order_by(order_by or [])
        limit_clause = f" LIMIT {limit}" if limit > 0 else ""
        offset_clause = f" OFFSET {offset}" if offset > 0 else ""

        sql = f"SELECT document FROM store WHERE {where_clause}{order_clause}{limit_clause}{offset_clause}"
        with self._get_connection() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [self._document_from_row(row[0]) for row in rows if row[0] is not None]

    def count_query(
        self,
        query: Dict[str, Any],
    ) -> int:
        where_clause, params = self._build_json_query(query)
        sql = f"SELECT COUNT(*) FROM store WHERE {where_clause}"
        with self._get_connection() as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else 0
