import hashlib
import json
import sqlite3
from contextlib import contextmanager


class Store:
    def __init__(self, path):
        self.connection = sqlite3.connect(path, timeout=10, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS raw_objects(
                digest TEXT PRIMARY KEY, payload BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS imports(
                id INTEGER PRIMARY KEY, source TEXT NOT NULL,
                digest TEXT NOT NULL REFERENCES raw_objects(digest),
                UNIQUE(source,digest));
            CREATE TABLE IF NOT EXISTS records(
                record_key TEXT PRIMARY KEY, payload TEXT NOT NULL,
                import_id INTEGER NOT NULL REFERENCES imports(id));
        """)

    @contextmanager
    def transaction(self):
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def ingest(self, source, raw, records):
        digest = hashlib.sha256(raw).hexdigest()
        with self.transaction():
            self.connection.execute("INSERT OR IGNORE INTO raw_objects VALUES (?,?)", (digest, raw))
            self.connection.execute("INSERT OR IGNORE INTO imports(source,digest) VALUES (?,?)", (source, digest))
            import_id = self.connection.execute("SELECT id FROM imports WHERE source=? AND digest=?", (source, digest)).fetchone()[0]
            for record in records:
                key = record.get('key')
                if not isinstance(key, str) or not key:
                    raise ValueError('记录缺少有效标识')
                encoded = json.dumps(record, ensure_ascii=False, sort_keys=True, allow_nan=False)
                self.connection.execute("INSERT INTO records VALUES (?,?,?) ON CONFLICT(record_key) DO UPDATE SET payload=excluded.payload, import_id=excluded.import_id", (key, encoded, import_id))
        return digest

    def records(self):
        return [json.loads(row[0]) for row in self.connection.execute("SELECT payload FROM records ORDER BY record_key")]

    def provenance(self, key):
        row = self.connection.execute("SELECT imports.source, imports.digest FROM records JOIN imports ON imports.id=records.import_id WHERE record_key=?", (key,)).fetchone()
        return dict(row) if row else None

    def close(self):
        self.connection.close()
