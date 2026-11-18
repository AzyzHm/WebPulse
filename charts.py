"""Draw the benchmark charts from results/benchmark.json (written by benchmark.py).

    pip install matplotlib
    python charts.py

Output (results/):
    throughput.png   pages per second for every configuration, grouped by model
    scaling.png      left: how threads / the asyncio semaphore scale
                     right: hybrid, more parser processes (asyncio sem 50 as reference)

Colors follow the entity (model), never the rank: sequential is neutral grey,
threaded blue, asyncio orange, hybrid aqua. Error bars are one standard deviation
across repetitions. Values are written on the marks, so identity and numbers never
rely on color alone.
"""
import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # no display needed
import matplotlib.pyplot as plt

RESULTS_DIR = Path("results")

COLORS = {
    "sequential": "#8a8985",
    "threaded": "#2a78d6",
    "asyncio": "#eb6834",
    "hybrid": "#1baf7a",
}
NAMES = {
    "sequential": "Sequential",
    "threaded": "Threads",
    "asyncio": "Asyncio (parse in loop)",
    "hybrid": "Hybrid (asyncio + processes)",
}
INK, INK_2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def style(ax) -> None:
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=INK_2, labelsize=9)
    ax.grid(axis="x" if ax.name == "rectilinear" and getattr(ax, "_hbar", False) else "y",
            color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def load() -> dict:
    path = RESULTS_DIR / "benchmark.json"
    if not path.exists():
        sys.exit(f"{path} not found - run `python benchmark.py` first")
    return json.loads(path.read_text())


def throughput_chart(data: dict) -> None:
    rows = data["summary"]
    meta = data["meta"]
    fig, ax = plt.subplots(figsize=(9, 0.42 * len(rows) + 1.6), facecolor=SURFACE)
    ax._hbar = True
    style(ax)

    ys = list(range(len(rows)))[::-1]  # first row at the top
    for y, r in zip(ys, rows):
        ax.barh(y, r["pps_mean"], height=0.62, color=COLORS[r["mode"]],
                xerr=r["pps_std"] if r["reps"] > 1 else None,
                error_kw={"ecolor": INK_2, "elinewidth": 1, "capsize": 3})
        sp = f"  {r['speedup']:.1f}x" if r.get("speedup") else ""
        ax.text(r["pps_mean"] + r["pps_std"] + max(x["pps_mean"] for x in rows) * 0.01, y,
                f"{r['pps_mean']:.0f} pages/s{sp}", va="center", fontsize=9, color=INK)

    ax.set_yticks(ys, [r["label"] for r in rows], color=INK)
    ax.set_xlabel("Pages per second (higher is better)", color=INK_2, fontsize=9)
    ax.set_xlim(0, max(r["pps_mean"] + r["pps_std"] for r in rows) * 1.28)
    ax.set_title(
        f"Crawl throughput by concurrency model ({meta['pages']} pages, "
        f"{meta['cpu_count']} CPUs)",
        loc="left", fontsize=12, color=INK, pad=12)
    handles = [plt.Rectangle((0, 0), 1, 1, color=c) for c in COLORS.values()]
    ax.legend(handles, NAMES.values(), loc="upper right", frameon=False,
              fontsize=9, labelcolor=INK_2)
    fig.tight_layout()
    fig.savefig(RESULTS_DIR / "throughput.png", dpi=200, facecolor=SURFACE)
    plt.close(fig)


def scaling_chart(data: dict) -> None:
    rows = data["summary"]
    meta = data["meta"]
    by_mode = {m: [r for r in rows if r["mode"] == m] for m in COLORS}
    fig, (left, right) = plt.subplots(1, 2, figsize=(11, 4.4), facecolor=SURFACE)
    for ax in (left, right):
        style(ax)

    # Left: I/O concurrency level vs throughput (threads and asyncio).
    for mode in ("threaded", "asyncio"):
        pts = by_mode[mode]
        xs = [r["param"] for r in pts]
        ys = [r["pps_mean"] for r in pts]
        errs = [r["pps_std"] for r in pts]
        left.errorbar(xs, ys, yerr=errs, color=COLORS[mode], linewidth=2, marker="o",
                      markersize=6, markeredgecolor=SURFACE, markeredgewidth=1.5,
                      ecolor=COLORS[mode], elinewidth=1, capsize=3, label=NAMES[mode])
        left.annotate(f"{ys[-1]:.0f}", (xs[-1], ys[-1]), textcoords="offset points",
                      xytext=(0, 9), ha="center", fontsize=9, color=INK)
    if by_mode["sequential"]:
        base = by_mode["sequential"][0]["pps_mean"]
        left.axhline(base, color=COLORS["sequential"], linewidth=1.5, linestyle=(0, (4, 3)))
        left.text(left.get_xlim()[1], base, f" sequential {base:.1f}", va="bottom",
                  ha="right", fontsize=9, color=INK_2)
    left.set_xscale("log")
    all_x = sorted({r["param"] for m in ("threaded", "asyncio") for r in by_mode[m]})
    left.set_xticks(all_x, [str(x) for x in all_x])
    left.minorticks_off()
    left.set_xlabel("Threads / semaphore size (log scale)", color=INK_2, fontsize=9)
    left.set_ylabel("Pages per second", color=INK_2, fontsize=9)
    left.set_title("I/O concurrency level vs throughput", loc="left", fontsize=11, color=INK)
    left.legend(frameon=False, fontsize=9, labelcolor=INK_2, loc="upper left")

    # Right: hybrid with 1/2/4/8 processes, asyncio at the same semaphore as reference.
    hy = by_mode["hybrid"]
    xs = [str(r["param"]) for r in hy]
    bars = right.bar(xs, [r["pps_mean"] for r in hy], width=0.5, color=COLORS["hybrid"],
                     yerr=[r["pps_std"] if r["reps"] > 1 else 0 for r in hy],
                     error_kw={"ecolor": INK_2, "elinewidth": 1, "capsize": 3})
    for bar, r in zip(bars, hy):
        right.text(bar.get_x() + bar.get_width() / 2, r["pps_mean"] + r["pps_std"] + 1,
                   f"{r['pps_mean']:.0f}", ha="center", va="bottom", fontsize=9, color=INK)
    ref = next((r for r in by_mode["asyncio"] if r["param"] == meta["hybrid_semaphore"]), None)
    if ref:
        right.axhline(ref["pps_mean"], color=COLORS["asyncio"], linewidth=1.5,
                      linestyle=(0, (4, 3)),
                      label=f"asyncio sem {ref['param']} (parse in loop): {ref['pps_mean']:.0f}")
        right.legend(frameon=False, fontsize=9, labelcolor=INK_2, loc="upper left")
    right.set_xlabel(f"Parser processes (semaphore {meta['hybrid_semaphore']})",
                     color=INK_2, fontsize=9)
    right.set_ylabel("Pages per second", color=INK_2, fontsize=9)
    right.set_title(f"Hybrid: more processes ({meta['cpu_count']} CPUs on this machine)",
                    loc="left", fontsize=11, color=INK)
    right.set_ylim(0, max(r["pps_mean"] + r["pps_std"] for r in hy) * 1.3)
    left.set_ylim(0, max(r["pps_mean"] + r["pps_std"] for m in ("threaded", "asyncio")
                         for r in by_mode[m]) * 1.15)

    fig.tight_layout()
    fig.savefig(RESULTS_DIR / "scaling.png", dpi=200, facecolor=SURFACE)
    plt.close(fig)


def main() -> None:
    data = load()
    throughput_chart(data)
    scaling_chart(data)
    print(f"wrote {RESULTS_DIR}/throughput.png and {RESULTS_DIR}/scaling.png")


if __name__ == "__main__":
    main()