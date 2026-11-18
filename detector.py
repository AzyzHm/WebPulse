"""Price change detection.

Compares the prices of one crawl with the most recent earlier price of the same
URL (from price_history) and records every difference in the `changes` table.

It runs after the crawl, once the storage thread has committed everything, and
is plain SQL on its own connection: no shared state, no threads.

Rules:
  - A URL is compared with its latest earlier priced observation, even if it
    was skipped in some crawls in between.
  - A change must be at least half a cent (prices have 2 decimals), and at
    least `min_pct` percent in absolute value (default 0 = any change).
  - URLs seen for the first time have nothing to compare with and are ignored.
  - Re-running detection for the same crawl replaces its previous results.
"""
import sqlite3
from datetime import datetime, timezone

_MIN_ABS_CHANGE = 0.005

_DETECT_SQL = """
INSERT INTO changes (crawl_id, url, title, old_price, new_price, pct_change, detected_at)
SELECT cur.crawl_id, cur.url, p.title, prev.price, cur.price,
       (cur.price - prev.price) * 100.0 / prev.price, :now
  FROM price_history AS cur
  JOIN pages AS p ON p.url = cur.url
  JOIN price_history AS prev
    ON prev.url = cur.url
   AND prev.crawl_id = (SELECT MAX(h.crawl_id) FROM price_history AS h
                         WHERE h.url = cur.url AND h.crawl_id < cur.crawl_id)
 WHERE cur.crawl_id = :crawl_id
   AND prev.price > 0
   AND ABS(cur.price - prev.price) >= :min_abs
   AND ABS((cur.price - prev.price) * 100.0 / prev.price) >= :min_pct
"""

_COMPARED_SQL = """
SELECT COUNT(*) FROM price_history AS cur
 WHERE cur.crawl_id = ?
   AND EXISTS (SELECT 1 FROM price_history AS h
                WHERE h.url = cur.url AND h.crawl_id < cur.crawl_id)
"""


def previous_crawl_id(conn: sqlite3.Connection, crawl_id: int) -> int | None:
    row = conn.execute("SELECT MAX(id) FROM crawls WHERE id < ?", (crawl_id,)).fetchone()
    return row[0]


def detect_changes(
    conn: sqlite3.Connection, crawl_id: int, min_pct: float = 0.0
) -> tuple[int, list[dict]]:
    """Detect and store price changes for `crawl_id`.

    Returns (compared, changes): how many priced URLs had an earlier price to
    compare with, and the changes found, biggest absolute % change first.
    """
    with conn:  # one transaction: delete old results + insert new ones
        conn.execute("DELETE FROM changes WHERE crawl_id = ?", (crawl_id,))
        conn.execute(
            _DETECT_SQL,
            {
                "now": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "crawl_id": crawl_id,
                "min_abs": _MIN_ABS_CHANGE,
                "min_pct": min_pct,
            },
        )
    compared = conn.execute(_COMPARED_SQL, (crawl_id,)).fetchone()[0]
    return compared, load_changes(conn, crawl_id)


def load_changes(
    conn: sqlite3.Connection, crawl_id: int, min_pct: float = 0.0
) -> list[dict]:
    rows = conn.execute(
        """
        SELECT url, title, old_price, new_price, pct_change
          FROM changes
         WHERE crawl_id = ? AND ABS(pct_change) >= ?
         ORDER BY ABS(pct_change) DESC, url
        """,
        (crawl_id, min_pct),
    ).fetchall()
    return [dict(r) for r in rows]


def format_changes(changes: list[dict], limit: int = 10) -> str:
    """Plain-text table of the biggest changes."""
    if not changes:
        return "  (no price changes)"
    lines = []
    for c in changes[:limit]:
        arrow = "▲" if c["pct_change"] > 0 else "▼"
        lines.append(
            f"  {arrow} {c['title'] or c['url']:<14} "
            f"${c['old_price']:>8.2f} -> ${c['new_price']:>8.2f}  "
            f"({c['pct_change']:+6.1f}%)"
        )
    if len(changes) > limit:
        lines.append(f"  ... and {len(changes) - limit} more")
    return "\n".join(lines)