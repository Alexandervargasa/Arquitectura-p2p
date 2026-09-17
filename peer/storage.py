"""
Almacenamiento local de cada peer, respaldado en SQLite.

Cada peer es, a nivel local, un almacen clave-valor: guarda "blobs"
(bytes) bajo una clave logica. Esa clave puede ser:
  - un chunk de archivo:      "chunk::<path>::<indice>"
  - los metadatos de archivo: "file::<path>"
  - el indice de un directorio: "dir::<path>"

No importa si, para una clave dada, este peer es el dueño primario o
solo guarda una replica: el storage local no distingue, esa logica vive
en el coordinador (peer/coordinator.py).
"""

import sqlite3
import threading


class BlobStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._local = threading.local()
        with self._conn() as c:
            c.execute(
                """CREATE TABLE IF NOT EXISTS blobs (
                       key TEXT PRIMARY KEY,
                       data BLOB NOT NULL,
                       updated_at REAL NOT NULL
                   )"""
            )

    def _conn(self):
        if not hasattr(self._local, "conn"):
            self._local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        return self._local.conn

    def put(self, key: str, data: bytes):
        import time
        with self._conn() as c:
            c.execute(
                "INSERT INTO blobs(key, data, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at",
                (key, data, time.time()),
            )

    def get(self, key: str):
        cur = self._conn().execute("SELECT data FROM blobs WHERE key = ?", (key,))
        row = cur.fetchone()
        return row[0] if row else None

    def delete(self, key: str) -> bool:
        with self._conn() as c:
            cur = c.execute("DELETE FROM blobs WHERE key = ?", (key,))
            return cur.rowcount > 0

    def exists(self, key: str) -> bool:
        cur = self._conn().execute("SELECT 1 FROM blobs WHERE key = ?", (key,))
        return cur.fetchone() is not None

    def count(self) -> int:
        cur = self._conn().execute("SELECT COUNT(*) FROM blobs")
        return cur.fetchone()[0]

    def keys(self):
        cur = self._conn().execute("SELECT key FROM blobs")
        return [r[0] for r in cur.fetchall()]
