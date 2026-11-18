"""Benchmark every concurrency model on the same crawl and compare them.

Runs, with several repetitions each (mean and spread are reported):
  - sequential                      baseline (1 request at a time)
  - threaded   workers     10 / 50 / 100
  - asyncio    semaphore    5 / 10 / 50 / 100   (parsing inside the event loop)
  - hybrid     processes    1 / 2 / 4 / 8       (asyncio fetch + process pool parse)

Every run is checked against the first one (same number of pages and same
URL list, as an md5 hash), so a speed-up can never come from silently
crawling less.

The script starts its own mock server (so latency, page size and port are the
same for every run) and stops it at the end. Use --no-server if you already
have one running.

Usage:
    python benchmark.py                       # full run, 492 pages
    python benchmark.py --quick               # short smoke run (100 pages, 1 rep)
    python benchmark.py --reps 5 --seq-reps 3
    python benchmark.py --skip-sequential     # reuse when you only change other configs

Output (results/):
    benchmark.json   every single run, plus the machine description
    benchmark.csv    one summary row per configuration
    benchmark.md     the same summary as a Markdown table
Then:  python charts.py   to draw the charts from benchmark.json.

Fair-comparison notes:
  - The mock server runs on the same machine, so it competes with the crawler
    for CPU. Close other heavy programs while benchmarking.
  - Hybrid speed-up with more processes is capped by your physical core count.
  - elapsed is the crawl time reported by each crawler. For hybrid it excludes
    process-pool start-up, which is recorded separately (pool_startup).
"""
import argparse
import csv
import hashlib
import json
import os
import platform
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import config

RESULTS_DIR = Path("results")

SEMAPHORES = (5, 10, 50, 100)
THREADS = (10, 50, 100)
PROCESSES = (1, 2, 4, 8)
HYBRID_SEMAPHORE = 50  # the asyncio curve flattens here, so processes are what varies


def url_hash(results: list[dict]) -> str:
    return hashlib.md5("\n".join(r["url"] for r in results).encode()).hexdigest()[:8]


def build_plan(args: argparse.Namespace) -> list[dict]:
    """The list of configurations to run, in a fixed order."""
    plan = []
    if not args.skip_sequential:
        plan.append({"mode": "sequential", "label": "sequential", "param": None,
                     "reps": args.seq_reps})
    for n in THREADS:
        plan.append({"mode": "threaded", "label": f"threaded {n}", "param": n,
                     "reps": args.reps})
    for n in SEMAPHORES:
        plan.append({"mode": "asyncio", "label": f"asyncio sem {n}", "param": n,
                     "reps": args.reps})
    for n in PROCESSES:
        plan.append({"mode": "hybrid", "label": f"hybrid {n} proc", "param": n,
                     "reps": args.reps})
    return plan


def run_once(cfg: dict, max_pages: int, max_depth: int) -> tuple[list[dict], dict]:
    """Run one crawl for a configuration. Imports are lazy (only what is needed)."""
    seeds, mode, p = config.SEEDS, cfg["mode"], cfg["param"]
    if mode == "sequential":
        import sequential
        return sequential.crawl(seeds, max_pages, max_depth)
    if mode == "threaded":
        import threaded
        return threaded.crawl(seeds, max_pages, max_depth, workers=p)
    if mode == "asyncio":
        import fetcher
        return fetcher.crawl(seeds, max_pages, max_depth, concurrency=p)
    import hybrid
    return hybrid.crawl(seeds, max_pages, max_depth, HYBRID_SEMAPHORE, processes=p)


# ---- mock server ---------------------------------------------------------------

def start_server(args: argparse.Namespace) -> subprocess.Popen:
    cmd = [sys.executable, "mock_server.py", "--epoch", "0",
           "--min-delay", str(args.min_delay), "--max-delay", str(args.max_delay)]
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait_for_server(proc)
    return proc


def wait_for_server(proc: subprocess.Popen | None = None, timeout: float = 15.0) -> None:
    url = f"{config.MOCK_BASE_URL}/robots.txt"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError("mock server exited at start-up (is port "
                               f"{config.MOCK_PORT} already in use?)")
        try:
            with urllib.request.urlopen(url, timeout=1):
                return
        except (urllib.error.URLError, OSError):
            time.sleep(0.2)
    raise RuntimeError(f"mock server not reachable at {config.MOCK_BASE_URL}")


# ---- statistics ------------------------------------------------------------------

def summarize(cfg: dict, runs: list[dict]) -> dict:
    elapsed = [r["elapsed"] for r in runs]
    rates = [r["pages"] / r["elapsed"] for r in runs]
    return {
        "label": cfg["label"],
        "mode": cfg["mode"],
        "param": cfg["param"],
        "reps": len(runs),
        "pages": runs[0]["pages"],
        "elapsed_mean": statistics.fmean(elapsed),
        "elapsed_std": statistics.stdev(elapsed) if len(elapsed) > 1 else 0.0,
        "elapsed_min": min(elapsed),
        "elapsed_max": max(elapsed),
        "pps_mean": statistics.fmean(rates),
        "pps_std": statistics.stdev(rates) if len(rates) > 1 else 0.0,
    }


def add_speedups(rows: list[dict]) -> None:
    """Speed-up relative to the sequential mean (None if sequential was skipped)."""
    base = next((r for r in rows if r["mode"] == "sequential"), None)
    for r in rows:
        r["speedup"] = base["elapsed_mean"] / r["elapsed_mean"] if base else None


def write_outputs(rows: list[dict], runs: list[dict], meta: dict) -> None:
    RESULTS_DIR.mkdir(exist_ok=True)
    with open(RESULTS_DIR / "benchmark.json", "w") as f:
        json.dump({"meta": meta, "summary": rows, "runs": runs}, f, indent=2)

    fields = ["label", "mode", "param", "reps", "pages", "elapsed_mean", "elapsed_std",
              "elapsed_min", "elapsed_max", "pps_mean", "pps_std", "speedup"]
    with open(RESULTS_DIR / "benchmark.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: (round(v, 4) if isinstance(v, float) else v)
                        for k, v in r.items() if k in fields})

    with open(RESULTS_DIR / "benchmark.md", "w") as f:
        f.write(markdown_table(rows, meta))


def markdown_table(rows: list[dict], meta: dict) -> str:
    lines = [
        f"Machine: {meta['cpu_count']} logical CPUs, {meta['platform']}, "
        f"Python {meta['python']}. {meta['pages']} pages per run, "
        f"mock latency {meta['min_delay']:.0f}-{meta['max_delay']:.0f} ms.",
        "",
        "| Configuration | Runs | Time (s) mean ± std | Pages/s mean ± std | Speed-up |",
        "|---|---:|---:|---:|---:|",
    ]
    for r in rows:
        sp = f"{r['speedup']:.1f}x" if r["speedup"] else "n/a"
        lines.append(
            f"| {r['label']} | {r['reps']} | {r['elapsed_mean']:.2f} ± {r['elapsed_std']:.2f} "
            f"| {r['pps_mean']:.1f} ± {r['pps_std']:.1f} | {sp} |"
        )
    return "\n".join(lines) + "\n"


# ---- main -------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="WebPulse benchmark")
    ap.add_argument("--reps", type=int, default=3, help="repetitions per configuration")
    ap.add_argument("--seq-reps", type=int, default=2,
                    help="repetitions for the (slow, stable) sequential baseline")
    ap.add_argument("--max-pages", type=int, default=500)
    ap.add_argument("--max-depth", type=int, default=config.MAX_DEPTH)
    ap.add_argument("--min-delay", type=float, default=config.MOCK_MIN_DELAY_MS)
    ap.add_argument("--max-delay", type=float, default=config.MOCK_MAX_DELAY_MS)
    ap.add_argument("--skip-sequential", action="store_true")
    ap.add_argument("--no-server", action="store_true",
                    help="use a mock server that is already running")
    ap.add_argument("--quick", action="store_true",
                    help="smoke run: 100 pages, 1 repetition, 1 sequential repetition")
    args = ap.parse_args()
    if args.quick:
        args.max_pages, args.reps, args.seq_reps = 100, 1, 1

    server = None
    try:
        if args.no_server:
            wait_for_server()
        else:
            server = start_server(args)

        plan = build_plan(args)
        total = sum(c["reps"] for c in plan)
        done = 0
        runs: list[dict] = []
        rows: list[dict] = []
        reference: tuple[int, str] | None = None
        print(f"{len(plan)} configurations, {total} runs, up to {args.max_pages} pages each\n")

        # Warm-up (not recorded): imports, connection set-up, first-run effects.
        for mode, p in (("threaded", 10), ("asyncio", 50), ("hybrid", 2)):
            run_once({"mode": mode, "param": p}, 30, args.max_depth)

        for cfg in plan:
            cfg_runs = []
            for rep in range(cfg["reps"]):
                results, stats = run_once(cfg, args.max_pages, args.max_depth)
                done += 1
                identity = (stats["pages"], url_hash(results))
                if reference is None:
                    reference = identity
                if identity != reference or stats["errors"]:
                    print(f"  !! {cfg['label']} run {rep + 1}: pages={stats['pages']} "
                          f"hash={identity[1]} errors={stats['errors']} "
                          f"(expected {reference[0]} / {reference[1]})")
                run = {"label": cfg["label"], "mode": cfg["mode"], "param": cfg["param"],
                       "rep": rep + 1, "pages": stats["pages"], "errors": stats["errors"],
                       "retries": stats.get("retries", 0), "elapsed": stats["elapsed"],
                       "pool_startup": stats.get("pool_startup"), "hash": identity[1],
                       "same_as_reference": identity == reference}
                cfg_runs.append(run)
                runs.append(run)
                print(f"[{done:>2}/{total}] {cfg['label']:<16} run {rep + 1}/{cfg['reps']}: "
                      f"{stats['elapsed']:6.2f}s  {stats['pages'] / stats['elapsed']:6.1f} pages/s")
            rows.append(summarize(cfg, cfg_runs))

        add_speedups(rows)
        meta = {
            "cpu_count": os.cpu_count(), "platform": platform.platform(),
            "python": platform.python_version(), "pages": reference[0],
            "reference_hash": reference[1], "min_delay": args.min_delay,
            "max_delay": args.max_delay, "hybrid_semaphore": HYBRID_SEMAPHORE,
            "all_runs_identical": all(r["same_as_reference"] for r in runs),
            "date": time.strftime("%Y-%m-%d"),
        }
        write_outputs(rows, runs, meta)
        print("\n" + markdown_table(rows, meta))
        print(f"results written to {RESULTS_DIR}/ "
              f"(identical page sets in every run: {meta['all_runs_identical']})")
        return 0 if meta["all_runs_identical"] else 1
    finally:
        if server is not None:
            server.terminate()
            server.wait(timeout=5)


if __name__ == "__main__":
    sys.exit(main())