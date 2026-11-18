"""Multithreaded crawler: a ThreadPoolExecutor of workers (requests library).

Same crawl logic and same crawl() contract as sequential.py, so the two can
be compared directly. Each worker thread downloads AND parses one page.

Why this is only a partial win: waiting on the network releases the GIL, so
many downloads overlap. But parse_page() is pure Python (CPU-bound) and holds
the GIL, so parsing in threads never runs in parallel. This is exactly the
limit the hybrid pipeline (asyncio fetch + process pool parse) removes.

Design notes:
- Only the main thread touches the visited set and the stats. Workers just
  return a result. Nothing is shared and mutated across threads, so no Lock
  is needed here (the Lock appears in phase 4, where the storage thread and
  other threads genuinely share counters).
- The crawl is level-by-level (see crawl()) so depths are exact and the page
  set matches the sequential baseline regardless of the worker count.

Run (with the mock server running in another terminal):
    python threaded.py
    python threaded.py --workers 20 --max-pages 100
"""
import argparse
import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from urllib import robotparser
from urllib.parse import urlparse

import requests

import config
from parser import parse_page
from sequential import load_robots

log = logging.getLogger("webpulse.threaded")

DEFAULT_WORKERS = 10

# requests.Session is not guaranteed thread-safe, so each worker thread gets
# its own session (which still reuses connections inside that thread).
_local = threading.local()


def _get_session() -> requests.Session:
    session = getattr(_local, "session", None)
    if session is None:
        session = requests.Session()
        session.headers["User-Agent"] = config.USER_AGENT
        _local.session = session
    return session


def fetch_and_parse(url: str) -> dict:
    """Worker task: download one page and parse it. Raises on HTTP/network errors."""
    resp = _get_session().get(url, timeout=config.REQUEST_TIMEOUT)
    resp.raise_for_status()
    return parse_page(resp.text, url)


def crawl(
    seeds: list[str],
    max_pages: int,
    max_depth: int,
    workers: int = DEFAULT_WORKERS,
    on_page: Callable[[dict], None] | None = None,
) -> tuple[list[dict], dict]:
    """Crawl with a thread pool. Returns (parsed_pages, stats).

    on_page, if given, is called from the main thread with every accepted
    page (used to stream pages to the storage thread).
    """
    visited: set[str] = set(seeds)
    robots_cache: dict[str, robotparser.RobotFileParser] = {}

    results: list[dict] = []
    stats = {"pages": 0, "errors": 0, "skipped_robots": 0}

    # robots.txt is fetched once per host, from the main thread, before any
    # worker is started for that host.
    robots_session = requests.Session()
    robots_session.headers["User-Agent"] = config.USER_AGENT

    start = time.perf_counter()
    level = list(seeds)  # all URLs at the current depth
    depth = 0

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="fetch") as pool:
        # Level-synchronous BFS: download a whole depth level in parallel,
        # wait for it to finish, then expand the next level. Processing
        # completions out of order would let a page be first discovered via a
        # slower, deeper path, record a depth that is too large, and silently
        # drop pages at the depth limit. Levels keep depths exact (same page
        # set as the sequential baseline) at the cost of a short wait at the
        # end of each level.
        while level and stats["pages"] < max_pages:
            to_fetch: list[str] = []
            for url in level:
                host = urlparse(url).netloc
                if host not in robots_cache:
                    robots_cache[host] = load_robots(robots_session, url)
                if not robots_cache[host].can_fetch(config.USER_AGENT, url):
                    stats["skipped_robots"] += 1
                    log.debug("robots.txt disallows %s", url)
                    continue
                to_fetch.append(url)

            # Never download more than the remaining page budget.
            to_fetch = to_fetch[: max_pages - stats["pages"]]

            # Submit everything, then read results in submission order so the
            # output order is deterministic (same as a sequential BFS).
            futures = [(url, pool.submit(fetch_and_parse, url)) for url in to_fetch]

            next_level: list[str] = []
            for url, future in futures:
                try:
                    page = future.result()
                except requests.RequestException as exc:
                    stats["errors"] += 1
                    log.warning("fetch failed %s: %s", url, exc)
                    continue

                results.append(page)
                stats["pages"] += 1
                if on_page:
                    on_page(page)

                if depth < max_depth:
                    host = urlparse(url).netloc
                    for link in page["links"]:
                        if urlparse(link).netloc == host and link not in visited:
                            visited.add(link)
                            next_level.append(link)

            level = next_level
            depth += 1

    robots_session.close()
    stats["elapsed"] = time.perf_counter() - start
    return results, stats


def main() -> None:
    parser = argparse.ArgumentParser(description="WebPulse threaded crawler")
    parser.add_argument("--seeds", nargs="+", default=config.SEEDS)
    parser.add_argument("--max-pages", type=int, default=config.MAX_PAGES)
    parser.add_argument("--max-depth", type=int, default=config.MAX_DEPTH)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    results, stats = crawl(args.seeds, args.max_pages, args.max_depth, args.workers)

    priced = [r for r in results if r["price"] is not None]
    print(f"\n=== Threaded ({args.workers} workers) ===")
    print(f"pages fetched:     {stats['pages']}")
    print(f"pages with price:  {len(priced)}")
    print(f"errors:            {stats['errors']}")
    print(f"skipped (robots):  {stats['skipped_robots']}")
    print(f"elapsed:           {stats['elapsed']:.2f}s")
    if stats["pages"]:
        print(f"pages per second:  {stats['pages'] / stats['elapsed']:.1f}")
    for page in priced[:3]:
        print(f"  e.g. {page['title']!r} -> ${page['price']:.2f} ({page['url']})")


if __name__ == "__main__":
    main()