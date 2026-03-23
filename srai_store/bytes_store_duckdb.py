import re
import zlib
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

import duckdb

from srai_store.bytes_store_base import BytesStoreBase


class BytesStoreDuckdb(BytesStoreBase):
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
                    value BLOB
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

    def _compress(self, data: bytes) -> bytes:
        return zlib.compress(data)

    def _decompress(self, data: bytes) -> bytes:
        return zlib.decompress(data)

    def _validate_key(self, key: str) -> None:
        if not re.match(r"^[a-zA-Z0-9_.\-/]+$", key):
            raise ValueError(f"Invalid characters in key: {key}")

    def mget(self, keys: Sequence[str]) -> List[Optional[bytes]]:
        values: List[Optional[bytes]] = []
        with self._get_connection() as conn:
            for key in keys:
                self._validate_key(key)
                row = conn.execute("SELECT value FROM store WHERE key = ?", [key]).fetchone()
                if row:
                    values.append(self._decompress(row[0]))
                else:
                    values.append(None)
        return values

    def mset(self, key_value_pairs: Sequence[Tuple[str, bytes]]) -> None:
        with self._get_connection() as conn:
            for key, value in key_value_pairs:
                self._validate_key(key)
                compressed_value = self._compress(value)
                conn.execute(
                    "INSERT OR REPLACE INTO store (key, value) VALUES (?, ?)",
                    [key, compressed_value],
                )

    def mdelete(self, keys: Sequence[str]) -> None:
        if not keys:
            return
        for key in keys:
            self._validate_key(key)
        placeholders = ",".join("?" * len(keys))
        with self._get_connection() as conn:
            conn.execute(f"DELETE FROM store WHERE key IN ({placeholders})", list(keys))

    def yield_keys(self, prefix: Optional[str] = None) -> Iterator[str]:
        with self._get_connection() as conn:
            if prefix:
                self._validate_key(prefix)
                result = conn.execute(
                    "SELECT key FROM store WHERE key LIKE ?",
                    [f"{prefix}%"],
                ).fetchall()
            else:
                result = conn.execute("SELECT key FROM store").fetchall()
            for row in result:
                yield row[0]

    def clear(self) -> None:
        with self._get_connection() as conn:
            conn.execute("DELETE FROM store")

    async def asample(self, count: int) -> List[bytes]:
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT value FROM store ORDER BY random() LIMIT ?",
                [count],
            ).fetchall()
            return [self._decompress(row[0]) for row in rows]

    def query(
        self,
        query: Dict[str, str],
        order_by: List[Tuple[str, bool]] = [],
        limit: int = 0,
        offset: int = 0,
    ) -> List[bytes]:
        raise NotImplementedError("Not implemented")

    def count_query(
        self,
        query: Dict[str, str],
    ) -> int:
        raise NotImplementedError("Not implemented")
