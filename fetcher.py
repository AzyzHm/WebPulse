"""Asyncio crawler: aiohttp + Semaphore, with timeouts, retries and backoff.

One thread, one event loop, hundreds of requests in flight. While a request
waits on the network the loop serves the others, so I/O-bound work overlaps
without the cost of one thread per request.

Same crawl logic and same result contract as sequential.py / threaded.py:
level-by-level BFS (exact depths, same page set), robots.txt check, depth and
page limits. crawl() is a plain synchronous wrapper so the benchmark script
can call every mode the same way.

Known limit, on purpose: parse_page() still runs inside the event loop here.
It is CPU-bound, so while one page is being parsed no other coroutine can run.
Phase 3 moves parsing to a process pool to remove that bottleneck.

Run (with the mock server running in another terminal):
    python fetcher.py
    python fetcher.py --concurrency 100 --max-pages 500
"""
import argparse
import asyncio
import logging
import random
import time
from urllib import robotparser
from urllib.parse import urlparse

import aiohttp

import config
from parser import parse_page

log = logging.getLogger("webpulse.fetcher")

# Errors worth retrying: network problems, timeouts, and 5xx / 429 responses.
_RETRY_STATUSES = {429, 500, 502, 503, 504}


async def fetch_html(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    url: str,
    stats: dict,
    max_retries: int = config.MAX_RETRIES,
) -> str:
    """Download one page, retrying transient failures with exponential backoff.

    The semaphore is held only while a request is actually in flight, NOT while
    sleeping between attempts, so a failing URL does not block a slot.
    Raises the last error once all attempts are used up.
    """
    for attempt in range(max_retries + 1):
        try:
            async with sem:
                async with session.get(url) as resp:
                    if resp.status in _RETRY_STATUSES:
                        raise aiohttp.ClientResponseError(
                            resp.request_info, resp.history, status=resp.status
                        )
                    resp.raise_for_status()  # other 4xx: fail without retrying
                    return await resp.text()
        except aiohttp.ClientResponseError as exc:
            if exc.status not in _RETRY_STATUSES:
                raise  # 404 and friends will not get better by retrying
            error: Exception = exc
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            error = exc

        if attempt == max_retries:
            raise error
        stats["retries"] += 1
        delay = config.BACKOFF_BASE * 2**attempt + random.uniform(0, 0.1)
        log.debug("retry %d for %s in %.2fs (%r)", attempt + 1, url, delay, error)
        await asyncio.sleep(delay)

    raise AssertionError("unreachable")  # pragma: no cover


async def load_robots(
    session: aiohttp.ClientSession, url: str
) -> robotparser.RobotFileParser:
    """Download and parse robots.txt for the host of `url`. Allow all on failure."""
    parts = urlparse(url)
    robots_url = f"{parts.scheme}://{parts.netloc}/robots.txt"
    rp = robotparser.RobotFileParser()
    try:
        async with session.get(robots_url) as resp:
            if resp.status == 200:
                rp.parse((await resp.text()).splitlines())
            else:
                rp.parse([])  # no usable robots.txt -> everything allowed
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        log.warning("could not fetch %s (%s), assuming allowed", robots_url, exc)
        rp.parse([])
    return rp


async def fetch_and_parse(
    session: aiohttp.ClientSession, sem: asyncio.Semaphore, url: str, stats: dict
) -> dict:
    html = await fetch_html(session, sem, url, stats)
    return parse_page(html, url)  # CPU-bound: blocks the event loop (see top)


async def crawl_async(
    seeds: list[str],
    max_pages: int,
    max_depth: int,
    concurrency: int = config.CONCURRENCY,
) -> tuple[list[dict], dict]:
    """Crawl with asyncio. Returns (parsed_pages, stats)."""
    visited: set[str] = set(seeds)
    robots_cache: dict[str, robotparser.RobotFileParser] = {}

    results: list[dict] = []
    stats = {"pages": 0, "errors": 0, "skipped_robots": 0, "retries": 0}

    # The semaphore (not the connector) is the single place that limits
    # concurrency, so connector limits are disabled. It is created inside the
    # running loop.
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

            # return_exceptions=True: one failed page must not cancel the
            # others. Results come back in input order (deterministic output).
            outcomes = await asyncio.gather(
                *(fetch_and_parse(session, sem, url, stats) for url in to_fetch),
                return_exceptions=True,
            )

            next_level: list[str] = []
            for url, outcome in zip(to_fetch, outcomes):
                if isinstance(outcome, BaseException):
                    stats["errors"] += 1
                    log.warning("fetch failed %s: %r", url, outcome)
                    continue

                results.append(outcome)
                stats["pages"] += 1

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
) -> tuple[list[dict], dict]:
    """Synchronous wrapper so callers (CLI, benchmark) need no event loop."""
    return asyncio.run(crawl_async(seeds, max_pages, max_depth, concurrency))


def main() -> None:
    parser = argparse.ArgumentParser(description="WebPulse asyncio crawler")
    parser.add_argument("--seeds", nargs="+", default=config.SEEDS)
    parser.add_argument("--max-pages", type=int, default=config.MAX_PAGES)
    parser.add_argument("--max-depth", type=int, default=config.MAX_DEPTH)
    parser.add_argument("--concurrency", type=int, default=config.CONCURRENCY)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    results, stats = crawl(args.seeds, args.max_pages, args.max_depth, args.concurrency)

    priced = [r for r in results if r["price"] is not None]
    print(f"\n=== Asyncio (semaphore {args.concurrency}) ===")
    print(f"pages fetched:     {stats['pages']}")
    print(f"pages with price:  {len(priced)}")
    print(f"errors:            {stats['errors']}")
    print(f"retries:           {stats['retries']}")
    print(f"skipped (robots):  {stats['skipped_robots']}")
    print(f"elapsed:           {stats['elapsed']:.2f}s")
    if stats["pages"]:
        print(f"pages per second:  {stats['pages'] / stats['elapsed']:.1f}")
    for page in priced[:3]:
        print(f"  e.g. {page['title']!r} -> ${page['price']:.2f} ({page['url']})")


if __name__ == "__main__":
    main()