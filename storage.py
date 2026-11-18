"""SQLite storage: one writer thread fed by a queue.Queue.

Why a dedicated writer thread:
  - SQLite allows one writer at a time. Funnelling every write through a single
    thread removes write contention and "database is locked" errors by design.
  - The crawl never waits for the disk: on_page() only appends to a queue, and
    the writer commits in batches while the crawler keeps fetching.

Thread model:
  - The writer thread opens its own sqlite3 connection (a connection must be
    used from the thread that created it) and is the only one that writes.
  - Producers (the crawler's main thread / event loop) call put(), which is a
    non-blocking queue.put().
  - A Lock protects the counters (queued / stored / failed) that are updated by
    producers and by the writer and read by whoever calls snapshot().
  - Shutdown is a sentinel message ("finish"), so everything queued before it
    is written and committed first.

A page that cannot be written (bad data, constraint error) is logged and
counted, and only that page is rolled back (SAVEPOINT); the rest of the batch
and the thread carry on.

Tables: crawls, pages (latest state per URL), price_history (one row per URL
per crawl), changes (filled by detector.py).
"""
import json
import logging
import queue
import sqlite3
import threading
import time
from datetime import datetime, timezone

import config

log = logging.getLogger("webpulse.storage")

SCHEMA = """
CREATE TABLE IF NOT EXISTS crawls (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at   TEXT NOT NULL,
    mode         TEXT NOT NULL,
    params       TEXT,
    pages        INTEGER,   -- pages the crawler reported
    errors       INTEGER,   -- fetch errors reported by the crawler
    elapsed      REAL,      -- crawl time in seconds
    stored       INTEGER,   -- pages actually written
    store_errors INTEGER    -- pages the writer failed to write
);
CREATE TABLE IF NOT EXISTS pages (
    url           TEXT PRIMARY KEY,
    title         TEXT,
    price         REAL,
    word_count    INTEGER,
    top_words     TEXT,     -- JSON list
    last_crawl_id INTEGER REFERENCES crawls(id),
    last_seen     TEXT
);
CREATE TABLE IF NOT EXISTS price_history (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    crawl_id INTEGER NOT NULL REFERENCES crawls(id),
    url      TEXT NOT NULL,
    price    REAL NOT NULL,
    seen_at  TEXT NOT NULL,
    UNIQUE (crawl_id, url)
);
CREATE INDEX IF NOT EXISTS idx_history_url ON price_history (url, crawl_id);
CREATE TABLE IF NOT EXISTS changes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    crawl_id    INTEGER NOT NULL REFERENCES crawls(id),
    url         TEXT NOT NULL,
    title       TEXT,
    old_price   REAL NOT NULL,
    new_price   REAL NOT NULL,
    pct_change  REAL NOT NULL,
    detected_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_changes_crawl ON changes (crawl_id);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect(db_path: str = config.DB_PATH) -> sqlite3.Connection:
    """Open a connection (rows as sqlite3.Row) and make sure the schema exists."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    return conn


class Storage:
    """Writer thread + queue. Use as a context manager:

        with Storage(db, mode="hybrid") as store:
            crawl(..., on_page=store.put)
            store.finish(stats)
        # leaving the block drains the queue, commits and joins the thread
    """

    def __init__(
        self,
        db_path: str = config.DB_PATH,
        mode: str = "unknown",
        params: dict | None = None,
        batch_size: int = 50,
        flush_interval: float = 0.2,
    ) -> None:
        self.db_path = db_path
        self.mode = mode
        self.params = params or {}
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.crawl_id: int | None = None

        # Unbounded on purpose: put() is called from the asyncio event loop,
        # which must never block. A crawl is thousands of small dicts at most.
        self._queue: queue.Queue = queue.Queue()
        self._lock = threading.Lock()
        self._counts = {"queued": 0, "stored": 0, "failed": 0}
        self._ready = threading.Event()
        self._error: BaseException | None = None
        self._finished = False
        self._thread = threading.Thread(
            target=self._run, name="storage-writer", daemon=True
        )

    # ---- public API (called from producer threads) ----------------------------

    def start(self) -> "Storage":
        self._thread.start()
        self._ready.wait()
        if self._error:
            raise RuntimeError(f"storage failed to start: {self._error!r}") from self._error
        return self

    def put(self, page: dict) -> None:
        """Queue one parsed page for writing. Non-blocking, thread-safe."""
        with self._lock:
            self._counts["queued"] += 1
        self._queue.put(("page", page))

    def finish(self, crawl_stats: dict | None = None) -> None:
        """Send the shutdown sentinel and wait until everything is on disk."""
        if self._finished:
            return
        self._finished = True
        self._queue.put(("finish", crawl_stats))
        self._thread.join()
        if self._error:
            raise RuntimeError(f"storage writer failed: {self._error!r}") from self._error

    def snapshot(self) -> dict:
        """Consistent copy of the counters, plus the current queue depth."""
        with self._lock:
            counts = dict(self._counts)
        counts["backlog"] = self._queue.qsize()
        return counts

    def __enter__(self) -> "Storage":
        return self.start()

    def __exit__(self, exc_type, exc, tb) -> None:
        # Always drain and close, even if the crawl raised, so what was
        # fetched so far is not lost and the thread does not linger.
        try:
            self.finish(None)
        except RuntimeError:
            if exc_type is None:
                raise

    # ---- writer thread --------------------------------------------------------

    def _run(self) -> None:
        # isolation_level=None -> autocommit; transactions are explicit
        # (BEGIN/COMMIT) so SAVEPOINTs behave predictably.
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, isolation_level=None)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(SCHEMA)
            cur = conn.execute(
                "INSERT INTO crawls (started_at, mode, params) VALUES (?, ?, ?)",
                (_now(), self.mode, json.dumps(self.params)),
            )
            self.crawl_id = cur.lastrowid
        except Exception as exc:  # noqa: BLE001 - reported to start()
            self._error = exc
            self._ready.set()
            if conn:
                conn.close()
            return
        self._ready.set()

        finish_stats: dict | None = None
        in_txn = False
        pending = 0
        try:
            while True:
                try:
                    kind, payload = self._queue.get(timeout=self.flush_interval)
                except queue.Empty:
                    # Idle: flush a partial batch so data is not held back.
                    if in_txn:
                        conn.execute("COMMIT")
                        in_txn, pending = False, 0
                    continue

                if kind == "finish":
                    finish_stats = payload
                    break

                if not in_txn:
                    conn.execute("BEGIN")
                    in_txn = True
                try:
                    self._write_page(conn, payload)
                    self._bump("stored")
                    pending += 1
                except Exception as exc:  # noqa: BLE001 - one bad page must not kill the thread
                    self._bump("failed")
                    log.warning("could not store %s: %r", payload.get("url"), exc)

                if pending >= self.batch_size:
                    conn.execute("COMMIT")
                    in_txn, pending = False, 0
        except Exception as exc:  # noqa: BLE001 - e.g. disk full; re-raised by finish()
            self._error = exc
            log.error("storage writer stopped: %r", exc)
        finally:
            try:
                if in_txn:
                    conn.execute("COMMIT")
                self._finalize_crawl(conn, finish_stats)
            except Exception as exc:  # noqa: BLE001
                self._error = self._error or exc
            conn.close()

    def _bump(self, key: str) -> None:
        with self._lock:
            self._counts[key] += 1

    def _write_page(self, conn: sqlite3.Connection, page: dict) -> None:
        now = _now()
        url, price = page["url"], page["price"]
        # SAVEPOINT: if either statement fails, only this page is rolled back.
        conn.execute("SAVEPOINT page")
        try:
            conn.execute(
                """
                INSERT INTO pages (url, title, price, word_count, top_words,
                                   last_crawl_id, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (url) DO UPDATE SET
                    title = excluded.title,
                    price = excluded.price,
                    word_count = excluded.word_count,
                    top_words = excluded.top_words,
                    last_crawl_id = excluded.last_crawl_id,
                    last_seen = excluded.last_seen
                """,
                (
                    url,
                    page["title"],
                    price,
                    page["word_count"],
                    json.dumps(page["top_words"]),
                    self.crawl_id,
                    now,
                ),
            )
            if price is not None:
                conn.execute(
                    """
                    INSERT INTO price_history (crawl_id, url, price, seen_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (crawl_id, url) DO UPDATE SET
                        price = excluded.price, seen_at = excluded.seen_at
                    """,
                    (self.crawl_id, url, price, now),
                )
            conn.execute("RELEASE page")
        except Exception:
            conn.execute("ROLLBACK TO page")
            conn.execute("RELEASE page")
            raise

    def _finalize_crawl(self, conn: sqlite3.Connection, crawl_stats: dict | None) -> None:
        with self._lock:
            counts = dict(self._counts)
        stats = crawl_stats or {}
        conn.execute(
            """
            UPDATE crawls
               SET pages = ?, errors = ?, elapsed = ?, stored = ?, store_errors = ?
             WHERE id = ?
            """,
            (
                stats.get("pages"),
                stats.get("errors"),
                stats.get("elapsed"),
                counts["stored"],
                counts["failed"],
                self.crawl_id,
            ),
        )


def main() -> None:
    """Tiny self-check: write a few pages from several threads and print the counts."""
    import tempfile
    from pathlib import Path

    logging.basicConfig(level=logging.INFO)
    with tempfile.TemporaryDirectory() as tmp:
        db = str(Path(tmp) / "demo.db")

        def producer(start: int) -> None:
            for i in range(start, start + 100):
                store.put(
                    {"url": f"http://x/{i}", "title": f"P{i}", "price": i + 0.5,
                     "word_count": 3, "top_words": ["a"]}
                )

        t0 = time.perf_counter()
        with Storage(db, mode="demo") as store:
            threads = [threading.Thread(target=producer, args=(n * 100,)) for n in range(4)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            store.finish({"pages": 400, "errors": 0, "elapsed": 0.0})
        print(store.snapshot(), f"{time.perf_counter() - t0:.2f}s")
        conn = connect(db)
        print("rows:", conn.execute("SELECT COUNT(*) FROM price_history").fetchone()[0])
        conn.close()


if __name__ == "__main__":
    main()