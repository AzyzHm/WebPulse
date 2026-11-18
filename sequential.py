"""Sequential baseline: one thread, one request at a time (requests library).

This is the reference every other mode (threads, asyncio, hybrid) is
measured against. It already contains the crawl logic we will keep reusing:
a URL frontier (deque), a visited set, a depth limit and a robots.txt check.

Run (with the mock server running in another terminal):
    python sequential.py
    python sequential.py --max-pages 50 --max-depth 1
"""
import argparse
import logging
import time
from collections import deque
from collections.abc import Callable
from urllib import robotparser
from urllib.parse import urlparse

import requests

import config
from parser import parse_page

log = logging.getLogger("webpulse.sequential")


def load_robots(session: requests.Session, url: str) -> robotparser.RobotFileParser:
    """Download and parse robots.txt for the host of `url`. Allow all on failure."""
    parts = urlparse(url)
    robots_url = f"{parts.scheme}://{parts.netloc}/robots.txt"
    rp = robotparser.RobotFileParser()
    try:
        resp = session.get(robots_url, timeout=config.REQUEST_TIMEOUT)
        if resp.status_code == 200:
            rp.parse(resp.text.splitlines())
        else:
            rp.parse([])  # no usable robots.txt -> everything allowed
    except requests.RequestException as exc:
        log.warning("could not fetch %s (%s), assuming allowed", robots_url, exc)
        rp.parse([])
    return rp


def crawl(
    seeds: list[str],
    max_pages: int,
    max_depth: int,
    on_page: Callable[[dict], None] | None = None,
) -> tuple[list[dict], dict]:
    """Crawl sequentially. Returns (parsed_pages, stats).

    on_page, if given, is called with every parsed page as soon as it is
    accepted (used to stream pages to the storage thread).
    """
    session = requests.Session()
    session.headers["User-Agent"] = config.USER_AGENT

    frontier: deque[tuple[str, int]] = deque((url, 0) for url in seeds)
    visited: set[str] = set(seeds)
    robots_cache: dict[str, robotparser.RobotFileParser] = {}

    results: list[dict] = []
    stats = {"pages": 0, "errors": 0, "skipped_robots": 0}

    start = time.perf_counter()
    while frontier and stats["pages"] < max_pages:
        url, depth = frontier.popleft()

        host = urlparse(url).netloc
        if host not in robots_cache:
            robots_cache[host] = load_robots(session, url)
        if not robots_cache[host].can_fetch(config.USER_AGENT, url):
            stats["skipped_robots"] += 1
            log.debug("robots.txt disallows %s", url)
            continue

        try:
            resp = session.get(url, timeout=config.REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as exc:
            stats["errors"] += 1
            log.warning("fetch failed %s: %s", url, exc)
            continue

        page = parse_page(resp.text, url)
        results.append(page)
        stats["pages"] += 1
        if on_page:
            on_page(page)

        if depth < max_depth:
            for link in page["links"]:
                if urlparse(link).netloc == host and link not in visited:
                    visited.add(link)
                    frontier.append((link, depth + 1))

    stats["elapsed"] = time.perf_counter() - start
    return results, stats


def main() -> None:
    parser = argparse.ArgumentParser(description="WebPulse sequential baseline")
    parser.add_argument("--seeds", nargs="+", default=config.SEEDS)
    parser.add_argument("--max-pages", type=int, default=config.MAX_PAGES)
    parser.add_argument("--max-depth", type=int, default=config.MAX_DEPTH)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    results, stats = crawl(args.seeds, args.max_pages, args.max_depth)

    priced = [r for r in results if r["price"] is not None]
    print("\n=== Sequential baseline ===")
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