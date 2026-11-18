"""Hybrid crawler: asyncio for the network, a process pool for the parsing.

Each model handles the kind of work it is good at:
  - I/O-bound (download): asyncio + aiohttp, hundreds of requests in flight on
    one thread, limited by a Semaphore (same fetch/retry code as fetcher.py).
  - CPU-bound (parse_page): ProcessPoolExecutor, one worker per core, so
    parsing runs in parallel and no longer blocks the event loop.

The glue is loop.run_in_executor(): a coroutine downloads a page, hands the
HTML to the pool, and awaits the result while the loop keeps serving the other
downloads. Downloads and parsing overlap.

Same crawl logic and result contract as the other modes: level-by-level BFS
(exact depths, same page set), robots.txt check, depth and page limits.

Windows notes: workers are started with `spawn`, so parse_page must stay a
top-level picklable function (it is) and the entry point must be guarded by
`if __name__ == "__main__":` (it is).

Run (with the mock server running in another terminal):
    python hybrid.py
    python hybrid.py --concurrency 100 --processes 4 --max-pages 500
"""
import argparse
import asyncio
import logging
import os
import time
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from urllib import robotparser
from urllib.parse import urlparse

import aiohttp

import config
from fetcher import fetch_html, load_robots
from parser import parse_page

log = logging.getLogger("webpulse.hybrid")

DEFAULT_PROCESSES = os.cpu_count() or 1


def _noop() -> None:
    """Top-level (picklable) task used to force worker start-up."""


def _warm_up(pool: ProcessPoolExecutor, processes: int) -> None:
    """Start every worker now, so process start-up is not billed to the crawl.

    Start-up cost (a few hundred ms with `spawn`, plus importing bs4/lxml in
    each worker) is paid once per run, not once per page. It is reported
    separately in stats["pool_startup"].
    """
    for future in [pool.submit(_noop) for _ in range(processes)]:
        future.result()


async def fetch_and_parse(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    pool: ProcessPoolExecutor,
    url: str,
    stats: dict,
) -> dict:
    html = await fetch_html(session, sem, url, stats)  # I/O: event loop
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(pool, parse_page, html, url)  # CPU: pool


async def crawl_async(
    seeds: list[str],
    max_pages: int,
    max_depth: int,
    pool: ProcessPoolExecutor,
    concurrency: int = config.CONCURRENCY,
    on_page: Callable[[dict], None] | None = None,
) -> tuple[list[dict], dict]:
    """Crawl with asyncio + a process pool. Returns (parsed_pages, stats).

    on_page, if given, is called from the event loop with every accepted page.
    It must not block (storage.put() only appends to a queue).
    """
    visited: set[str] = set(seeds)
    robots_cache: dict[str, robotparser.RobotFileParser] = {}

    results: list[dict] = []
    stats = {"pages": 0, "errors": 0, "skipped_robots": 0, "retries": 0}

    sem = asyncio.Semaphore(concurrency)
    timeout = aiohttp.ClientTimeout(total=config.REQUEST_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=0)
    headers = {"User-Agent": config.USER_AGENT}

    start = time.perf_counter()
    level = list(seeds)
    depth = 0

    async with aiohttp.ClientSession(
        headers=headers, timeout=timeout, connector=connector
    ) as session:
        while level and stats["pages"] < max_pages:
            to_fetch: list[str] = []
            for url in level:
                host = urlparse(url).netloc
                if host not in robots_cache:
                    robots_cache[host] = await load_robots(session, url)
                if not robots_cache[host].can_fetch(config.USER_AGENT, url):
                    stats["skipped_robots"] += 1
                    log.debug("robots.txt disallows %s", url)
                    continue
                to_fetch.append(url)

            to_fetch = to_fetch[: max_pages - stats["pages"]]

            outcomes = await asyncio.gather(
                *(fetch_and_parse(session, sem, pool, url, stats) for url in to_fetch),
                return_exceptions=True,
            )

            next_level: list[str] = []
            for url, outcome in zip(to_fetch, outcomes):
                if isinstance(outcome, BaseException):
                    stats["errors"] += 1
                    log.warning("fetch/parse failed %s: %r", url, outcome)
                    continue

                results.append(outcome)
                stats["pages"] += 1
                if on_page:
                    on_page(outcome)

                if depth < max_depth:
                    host = urlparse(url).netloc
                    for link in outcome["links"]:
                        if urlparse(link).netloc == host and link not in visited:
                            visited.add(link)
                            next_level.append(link)

            level = next_level
            depth += 1

    stats["elapsed"] = time.perf_counter() - start
    return results, stats


def crawl(
    seeds: list[str],
    max_pages: int,
    max_depth: int,
    concurrency: int = config.CONCURRENCY,
    processes: int = DEFAULT_PROCESSES,
    on_page: Callable[[dict], None] | None = None,
) -> tuple[list[dict], dict]:
    """Synchronous wrapper so callers (CLI, benchmark) need no event loop."""
    t0 = time.perf_counter()
    with ProcessPoolExecutor(max_workers=processes) as pool:
        _warm_up(pool, processes)
        startup = time.perf_counter() - t0
        results, stats = asyncio.run(
            crawl_async(seeds, max_pages, max_depth, pool, concurrency, on_page)
        )
    stats["pool_startup"] = startup
    return results, stats


def main() -> None:
    parser = argparse.ArgumentParser(description="WebPulse hybrid crawler")
    parser.add_argument("--seeds", nargs="+", default=config.SEEDS)
    parser.add_argument("--max-pages", type=int, default=config.MAX_PAGES)
    parser.add_argument("--max-depth", type=int, default=config.MAX_DEPTH)
    parser.add_argument("--concurrency", type=int, default=config.CONCURRENCY)
    parser.add_argument("--processes", type=int, default=DEFAULT_PROCESSES)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    results, stats = crawl(
        args.seeds, args.max_pages, args.max_depth, args.concurrency, args.processes
    )

    priced = [r for r in results if r["price"] is not None]
    print(f"\n=== Hybrid (semaphore {args.concurrency}, {args.processes} processes) ===")
    print(f"pages fetched:     {stats['pages']}")
    print(f"pages with price:  {len(priced)}")
    print(f"errors:            {stats['errors']}")
    print(f"retries:           {stats['retries']}")
    print(f"skipped (robots):  {stats['skipped_robots']}")
    print(f"elapsed:           {stats['elapsed']:.2f}s (+{stats['pool_startup']:.2f}s pool start-up)")
    if stats["pages"]:
        print(f"pages per second:  {stats['pages'] / stats['elapsed']:.1f}")
    for page in priced[:3]:
        print(f"  e.g. {page['title']!r} -> ${page['price']:.2f} ({page['url']})")


if __name__ == "__main__":
    main()