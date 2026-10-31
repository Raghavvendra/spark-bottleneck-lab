"""Run the whole lab and write evidence to results/.

    python -m bottleneck_lab.run_lab --reps 3                 # all scenarios
    python -m bottleneck_lab.run_lab --only skew_join --reps 3
    python -m bottleneck_lab.run_lab --cores 4 --scale 3      # bigger box, bigger data

For every (scenario, variant) it runs N fresh JVMs, keeps the MEDIAN run, parses that
run's Spark event log, diagnoses it, and compares it against the `bad` baseline.
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import shutil
import statistics
import subprocess
import sys
from typing import Dict, List

from .compare import compare
from .diagnose import diagnose, render_markdown
from .eventlog import parse_event_log
from .scenarios import SCENARIOS

# What each scenario INJECTS, so we can test whether the diagnoser names it.
EXPECTED = {
    "skew_join": {"skew"},
    "small_files": {"small_files"},
    "over_partitioned": {"tiny_tasks"},
    "python_udf": {"python_udf"},
    "join_strategy": {"broadcast_candidate"},
    "spill": {"spill"},
}


def run_one(scenario: str, variant: str, rep: int, data: str, cores: int, root: str) -> dict:
    out = os.path.join(root, "runs", f"{scenario}_{variant}_{rep}")
    cmd = [sys.executable, "-m", "bottleneck_lab.scenarios", "run", "--scenario", scenario,
           "--variant", variant, "--data", data, "--out", out, "--cores", str(cores)]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"{scenario}/{variant} failed:\n{r.stderr[-2000:]}")
    return json.load(open(os.path.join(out, "meta.json")))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data")
    ap.add_argument("--results", default="results")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--cores", type=int, default=1)
    ap.add_argument("--target-slots", type=int, default=16)
    ap.add_argument("--only", nargs="*")
    a = ap.parse_args()

    os.makedirs(os.path.join(a.results, "scenarios"), exist_ok=True)
    os.makedirs(os.path.join(a.results, "eventlogs"), exist_ok=True)
    data_meta = json.load(open(os.path.join(a.data, "meta.json")))
    all_out: Dict[str, dict] = {}

    for name, sc in SCENARIOS.items():
        if a.only and name not in a.only:
            continue
        print(f"\n=== {name}: {sc.what}")
        per_variant: Dict[str, dict] = {}
        for variant in sc.variants:
            metas = [run_one(name, variant, i, a.data, a.cores, a.results) for i in range(1, a.reps + 1)]
            walls = [m["wall_s"] for m in metas]
            med = sorted(metas, key=lambda m: m["wall_s"])[len(metas) // 2]
            app = parse_event_log(med["eventlog"])
            diag = diagnose(app, a.target_slots, tuple(med["window_ms"]))
            per_variant[variant] = {"walls": walls, "median_wall": statistics.median(walls),
                                    "checksum": med["checksum"], "diag": diag}
            # keep only the median run's event log, gzipped, as evidence
            dst = os.path.join(a.results, "eventlogs", f"{name}__{variant}.log.gz")
            with open(med["eventlog"], "rb") as fi, gzip.open(dst, "wb") as fo:
                shutil.copyfileobj(fi, fo)
            print(f"  {variant:16s} median {statistics.median(walls):6.2f}s  "
                  f"runs={[round(w, 1) for w in walls]}  findings={[f.kind for f in diag.findings]}")
        for m in os.listdir(os.path.join(a.results, "runs")):
            if m.startswith(name + "_"):
                shutil.rmtree(os.path.join(a.results, "runs", m), ignore_errors=True)

        base = per_variant["bad"]
        comps = {}
        for v, pv in per_variant.items():
            if v == "bad":
                continue
            comps[v] = compare(v, base["diag"], pv["diag"], base["median_wall"], pv["median_wall"],
                               base["checksum"], pv["checksum"])
        detected = {f.kind for f in base["diag"].findings}
        exp = EXPECTED[name]
        all_out[name] = {
            "what": sc.what,
            "expected": sorted(exp),
            "detected_on_bad": sorted(detected),
            "diagnoser_hit": exp <= detected,
            "false_alarms_on_variants": {v: sorted(exp & {f.kind for f in pv["diag"].findings})
                                         for v, pv in per_variant.items() if v != "bad"},
            "variants": {v: {"walls": pv["walls"], "median_wall": pv["median_wall"],
                             "checksum": pv["checksum"], "diagnosis": pv["diag"].to_dict()}
                         for v, pv in per_variant.items()},
            "comparisons": {v: c.__dict__ for v, c in comps.items()},
        }
        json.dump(all_out[name], open(os.path.join(a.results, "scenarios", f"{name}.json"), "w"), indent=1)
        with open(os.path.join(a.results, "scenarios", f"{name}.md"), "w") as fh:
            fh.write(f"# {name}\n\n{sc.what}\n\n")
            fh.write("## Diagnosis of the BAD run (what the tool says, from the event log only)\n\n")
            fh.write(render_markdown(base["diag"]))
            for v, c in comps.items():
                fh.write(f"\n## Re-measure: `{v}`\n\n")
                fh.write(f"- wall-clock (median of {a.reps}): **{c.wall_before_s:.2f}s -> {c.wall_after_s:.2f}s** "
                         f"({c.speedup_measured:.2f}x)\n")
                fh.write(f"- model estimate on {a.target_slots} slots (UNVALIDATED, ignore for conclusions): "
                         f"{c.projected_before_s:.2f}s -> {c.projected_after_s:.2f}s ({c.speedup_projected:.2f}x)\n")
                fh.write(f"- result checksum identical: **{c.result_identical}**\n")
                fh.write(f"- binding resource: {c.binding_before}  ->  {c.binding_after}\n")
                fh.write(f"- findings resolved: {c.resolved or 'none'}; still present: {c.remaining or 'none'}; "
                         f"newly introduced: {c.introduced or 'none'}\n")
                fh.write(f"- **verdict:** {c.verdict}\n")
                fh.write("\n<details><summary>Diagnosis of this variant</summary>\n\n")
                fh.write(render_markdown(per_variant[v]["diag"]))
                fh.write("\n</details>\n")

    meta = {"cores": a.cores, "reps": a.reps, "target_slots": a.target_slots, "data": data_meta}
    # assemble from every finished scenario on disk, so partial / resumed runs still merge
    merged = {}
    for name in SCENARIOS:
        pth = os.path.join(a.results, "scenarios", f"{name}.json")
        if os.path.exists(pth):
            merged[name] = json.load(open(pth))
    json.dump({"meta": meta, "scenarios": merged}, open(os.path.join(a.results, "summary.json"), "w"), indent=1)
    print("\nwrote", os.path.join(a.results, "summary.json"))


if __name__ == "__main__":
    main()
