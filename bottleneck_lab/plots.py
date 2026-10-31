"""Turn results/summary.json + the saved event logs into figures.

    python -m bottleneck_lab.plots --results results
"""
from __future__ import annotations

import argparse
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from .diagnose import BUCKETS, BUCKET_MEANING
from .eventlog import parse_event_log

COLORS = {"cpu": "#4C78A8", "gc": "#E45756", "fetch_wait": "#72B7B2",
          "overhead": "#F58518", "other_wait": "#B0B0B0"}
SHORT = {"cpu": "CPU", "gc": "GC", "fetch_wait": "shuffle fetch wait",
         "overhead": "per-task overhead", "other_wait": "other wait (I/O, spill, Python)"}


def _dominant_task_runs(results: str, scenario: str, variant: str, dom_stage: int):
    path = os.path.join(results, "eventlogs", f"{scenario}__{variant}.log.gz")
    app = parse_event_log(path)
    st = app.stages.get(dom_stage)
    return sorted((t.run_ms / 1000 for t in st.tasks), reverse=True) if st else []


def scenario_figure(results: str, name: str, sc: dict, target: int) -> str:
    variants = list(sc["variants"].keys())
    fig, axes = plt.subplots(1, 3, figsize=(17, 4.8))

    # A: measured wall-clock (median of runs, whiskers = min/max)
    ax = axes[0]
    xs = range(len(variants))
    med = [sc["variants"][v]["median_wall"] for v in variants]
    lo = [min(sc["variants"][v]["walls"]) for v in variants]
    hi = [max(sc["variants"][v]["walls"]) for v in variants]
    ax.bar(list(xs), med, 0.55, color="#4C78A8",
           yerr=[[m - l for m, l in zip(med, lo)], [h - m for m, h in zip(med, hi)]], capsize=3)
    for x, m in zip(xs, med):
        ax.text(x, m, f"{m:.1f}s", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(list(xs)); ax.set_xticklabels(variants, rotation=15)
    ax.set_ylabel("seconds"); ax.set_title("Wall-clock (measured, median of runs)")

    # B: where the task time went (stacked, all stages in window)
    ax = axes[1]
    bottoms = [0.0] * len(variants)
    for b in BUCKETS:
        vals = [sum(s["buckets_ms"][b] for s in sc["variants"][v]["diagnosis"]["stages"]) / 1000
                for v in variants]
        ax.bar(list(xs), vals, bottom=bottoms, color=COLORS[b], label=SHORT[b])
        bottoms = [a + c for a, c in zip(bottoms, vals)]
    ax.set_xticks(list(xs)); ax.set_xticklabels(variants, rotation=15)
    ax.set_ylabel("task-seconds (summed over all tasks)")
    ax.set_title("Where the time went (all stages)")
    ax.legend(fontsize=7)

    # C: task run-time profile of the baseline's dominant stage vs each variant's dominant stage
    ax = axes[2]
    for v in variants:
        d = sc["variants"][v]["diagnosis"]
        if d["dominant_stage"] is None:
            continue
        runs = _dominant_task_runs(results, name, v, d["dominant_stage"])
        if runs:
            ax.plot(range(1, len(runs) + 1), runs, marker="o" if len(runs) < 40 else None,
                    ms=3, lw=1.6, label=f"{v} (stage {d['dominant_stage']}, {len(runs)} tasks)")
    ax.set_yscale("log"); ax.set_xscale("log")
    ax.set_xlabel("task rank (slowest first)"); ax.set_ylabel("task run time (s)")
    ax.set_title("Dominant stage: task run-time distribution")
    ax.legend(fontsize=7)
    fig.suptitle(f"{name}: {sc['what']}", fontsize=11)
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    out = os.path.join(results, "figures", f"{name}.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    fig.savefig(out, dpi=130)
    plt.close(fig)
    return out


def summary_figure(results: str, data: dict) -> str:
    rows = []
    for name, sc in data["scenarios"].items():
        for v, c in sc["comparisons"].items():
            rows.append((f"{name} / {v}", c["speedup_measured"], c["result_identical"]))
    fig, ax = plt.subplots(figsize=(10, 0.5 * len(rows) + 1.8))
    ys = range(len(rows))
    colors = ["#E45756" if r[1] < 0.95 else "#4C78A8" for r in rows]
    ax.barh(list(ys), [r[1] for r in rows], 0.6, color=colors)
    ax.axvline(1.0, color="black", lw=1)
    ax.set_yticks(list(ys)); ax.set_yticklabels([r[0] for r in rows], fontsize=8)
    ax.invert_yaxis(); ax.set_xscale("log")
    ax.set_xlabel("measured speed-up vs `bad` baseline (log scale; <1 = the 'fix' made it worse)")
    for y, r in zip(ys, rows):
        ax.text(r[1] * 1.04, y, f"{r[1]:.2f}x" + ("" if r[2] else "  RESULT CHANGED"), va="center", fontsize=8)
    ax.set_title(f"Every fix, re-measured ({data['meta']['cores']} core(s), median of {data['meta']['reps']} fresh JVMs)")
    fig.tight_layout()
    out = os.path.join(results, "figures", "summary.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    fig.savefig(out, dpi=130)
    plt.close(fig)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default="results")
    a = ap.parse_args()
    data = json.load(open(os.path.join(a.results, "summary.json")))
    for name, sc in data["scenarios"].items():
        print(scenario_figure(a.results, name, sc, data["meta"]["target_slots"]))
    print(summary_figure(a.results, data))


if __name__ == "__main__":
    main()
