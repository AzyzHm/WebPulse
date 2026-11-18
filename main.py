"""WebPulse command line: crawl, store, detect price changes, report.

    python main.py crawl                       # hybrid mode, defaults
    python main.py crawl --mode threaded --workers 20 --max-pages 500
    python main.py crawl --mode hybrid --concurrency 100 --processes 4
    python main.py changes                     # changes found by the latest crawl
    python main.py changes --crawl-id 2 --min-change 10 --limit 20
    python main.py history http://127.0.0.1:8000/product/7

Typical demo (mock server running in another terminal):
    python mock_server.py --epoch 0     ->  python main.py crawl --max-pages 500
    python mock_server.py --epoch 1     ->  python main.py crawl --max-pages 500
The second crawl compares itself with the first and lists the changed prices.

Pipeline: crawler (asyncio / threads / processes) -> on_page -> Storage queue
-> writer thread -> SQLite; after the crawl, detector.py compares prices with
the previous crawl.
"""
import argparse
import logging
import sys

import config
import detector
import storage

MODES = ("sequential", "threaded", "asyncio", "hybrid")


def run_crawler(args: argparse.Namespace, on_page):
    """Call the chosen crawler with the arguments it understands."""
    seeds, max_pages, max_depth = args.seeds, args.max_pages, args.max_depth
    # Imported lazily so e.g. --mode asyncio does not need `requests` loaded.
    if args.mode == "sequential":
        import sequential
        return sequential.crawl(seeds, max_pages, max_depth, on_page=on_page)
    if args.mode == "threaded":
        import threaded
        return threaded.crawl(seeds, max_pages, max_depth, args.workers, on_page=on_page)
    if args.mode == "asyncio":
        import fetcher
        return fetcher.crawl(seeds, max_pages, max_depth, args.concurrency, on_page=on_page)
    import hybrid
    return hybrid.crawl(
        seeds, max_pages, max_depth, args.concurrency, args.processes, on_page=on_page
    )


def cmd_crawl(args: argparse.Namespace) -> int:
    params = {
        "seeds": args.seeds,
        "max_pages": args.max_pages,
        "max_depth": args.max_depth,
        "workers": args.workers,
        "concurrency": args.concurrency,
        "processes": args.processes,
    }
    with storage.Storage(args.db, mode=args.mode, params=params) as store:
        results, stats = run_crawler(args, store.put)
        store.finish(stats)  # drain the queue, commit, join the writer thread
    snap = store.snapshot()
    crawl_id = store.crawl_id

    conn = storage.connect(args.db)
    try:
        compared, changes = detector.detect_changes(conn, crawl_id, args.min_change)
    finally:
        conn.close()

    priced = sum(1 for r in results if r["price"] is not None)
    print(f"\n=== Crawl #{crawl_id} ({args.mode}) ===")
    print(f"pages fetched:     {stats['pages']} ({priced} with a price)")
    print(f"errors:            {stats['errors']}")
    print(f"skipped (robots):  {stats['skipped_robots']}")
    print(f"stored:            {snap['stored']} ({snap['failed']} failed)")
    print(f"elapsed:           {stats['elapsed']:.2f}s")
    if stats["pages"]:
        print(f"pages per second:  {stats['pages'] / stats['elapsed']:.1f}")
    print(f"database:          {args.db}")

    if compared == 0:
        print("\nPrice changes:     first crawl of these URLs, baseline stored "
              "(crawl again to detect changes)")
    else:
        up = sum(1 for c in changes if c["pct_change"] > 0)
        print(f"\nPrice changes:     {len(changes)} of {compared} compared "
              f"({up} up, {len(changes) - up} down)")
        print(detector.format_changes(changes, args.limit))
    return 0 if snap["failed"] == 0 else 1


def cmd_changes(args: argparse.Namespace) -> int:
    conn = storage.connect(args.db)
    try:
        crawl_id = args.crawl_id
        if crawl_id is None:
            crawl_id = conn.execute("SELECT MAX(id) FROM crawls").fetchone()[0]
        if crawl_id is None:
            print("no crawls in the database yet, run `python main.py crawl` first")
            return 1
        changes = detector.load_changes(conn, crawl_id, args.min_change)
        prev = detector.previous_crawl_id(conn, crawl_id)
    finally:
        conn.close()

    print(f"=== Price changes in crawl #{crawl_id}"
          + (f" (vs #{prev})" if prev else " (no earlier crawl)") + " ===")
    print(detector.format_changes(changes, args.limit))
    return 0


def cmd_history(args: argparse.Namespace) -> int:
    conn = storage.connect(args.db)
    try:
        rows = conn.execute(
            "SELECT crawl_id, price, seen_at FROM price_history WHERE url = ? ORDER BY crawl_id",
            (args.url,),
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        print(f"no price history for {args.url}")
        return 1
    print(f"=== Price history: {args.url} ===")
    for r in rows:
        print(f"  crawl #{r['crawl_id']:<3} ${r['price']:>9.2f}   {r['seen_at']}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="WebPulse: concurrent price/news monitor")
    parser.add_argument("--db", default=config.DB_PATH, help="SQLite file (default: %(default)s)")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    crawl = sub.add_parser("crawl", help="crawl, store and detect price changes")
    crawl.add_argument("--mode", choices=MODES, default="hybrid")
    crawl.add_argument("--seeds", nargs="+", default=config.SEEDS)
    crawl.add_argument("--max-pages", type=int, default=config.MAX_PAGES)
    crawl.add_argument("--max-depth", type=int, default=config.MAX_DEPTH)
    crawl.add_argument("--workers", type=int, default=10, help="threaded mode: thread count")
    crawl.add_argument("--concurrency", type=int, default=config.CONCURRENCY,
                       help="asyncio / hybrid: semaphore size")
    crawl.add_argument("--processes", type=int, default=None,
                       help="hybrid: parser processes (default: CPU count)")
    crawl.add_argument("--min-change", type=float, default=0.0,
                       help="only report changes of at least this many percent")
    crawl.add_argument("--limit", type=int, default=10, help="rows to print")
    crawl.set_defaults(func=cmd_crawl)

    changes = sub.add_parser("changes", help="show stored price changes")
    changes.add_argument("--crawl-id", type=int, default=None, help="default: latest crawl")
    changes.add_argument("--min-change", type=float, default=0.0)
    changes.add_argument("--limit", type=int, default=20)
    changes.set_defaults(func=cmd_changes)

    history = sub.add_parser("history", help="price history of one URL")
    history.add_argument("url")
    history.set_defaults(func=cmd_history)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if args.command == "crawl" and args.processes is None:
        import os
        args.processes = os.cpu_count() or 1
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())