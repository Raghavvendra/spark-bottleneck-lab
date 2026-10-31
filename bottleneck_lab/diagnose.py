"""Bottleneck diagnosis for a Spark application, from its event log alone.

Procedure (mirrors how an experienced engineer actually works):

  0. Define the window       - wall-clock of the workload, and how much of it
                               is NOT inside any stage (driver / planning gaps).
  1. WHERE                   - critical path through the stage DAG. Optimising a
                               stage off the critical path buys nothing (Amdahl).
  2. WHAT KIND               - break the dominant stage's task time into buckets
                               (CPU, GC, shuffle-fetch wait, per-task overhead,
                               other waiting) -> the binding resource.
  3. WHY                     - pathology rules (skew, tiny tasks, small files,
                               spill, Python UDF, join strategy), each with the
                               numbers that triggered it.
  4. FIX + RE-MEASURE        - see compare.py; the lab re-runs after the fix and
                               checks that the result is identical and that the
                               bottleneck actually moved.

Every threshold below is a documented heuristic, not a law. They are module
constants so you can tune them for your own workloads.
"""
from __future__ import annotations

import heapq
import statistics
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional

from .eventlog import AppLog, Stage

# ---- tunable heuristics -----------------------------------------------------
SKEW_RATIO = 4.0              # max task run time / median
SKEW_MIN_MAX_MS = 1000        # ignore "skew" among sub-second tasks (noise)
SKEW_MIN_DATA_RATIO = 2.0     # ...and the biggest task must also hold >= 2x the median data
TINY_TASK_MEDIAN_MS = 150     # median task duration below this = "tiny"
TINY_TASK_OVERHEAD_SHARE = 0.15
SMALL_FILE_AVG_BYTES = 8 * 1024 * 1024
SMALL_FILE_MIN_COUNT = 64
GC_FLAG_SHARE = 0.15
BROADCAST_RATIO = 20          # big side / small side shuffle bytes
BROADCAST_SMALL_BYTES = 128 * 1024 * 1024
MIN_STAGE_SHARE = 0.10        # analyse stages on the critical path above this

BUCKETS = ["cpu", "gc", "fetch_wait", "overhead", "other_wait"]

BUCKET_MEANING = {
    "cpu": "JVM compute (executorCpuTime)",
    "gc": "Garbage collection pauses",
    "fetch_wait": "Waiting for shuffle blocks (network / remote disk)",
    "overhead": "Per-task overhead (deserialise + scheduler delay + result ser.)",
    "other_wait": "Other waiting inside the task (storage I/O, spill I/O, local shuffle reads, Python workers)",
}


# ---- helpers ----------------------------------------------------------------
def _median(xs: List[float]) -> float:
    return statistics.median(xs) if xs else 0.0


def greedy_makespan(durations: List[float], slots: int) -> float:
    """Spark-like scheduling: tasks in launch order, each to the first free slot."""
    if not durations:
        return 0.0
    heap = [0.0] * max(1, slots)
    heapq.heapify(heap)
    for d in durations:
        free = heapq.heappop(heap)
        heapq.heappush(heap, free + d)
    return max(heap)


def _fmt_bytes(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} TiB"


def _to_num(v) -> Optional[float]:
    try:
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return None


# ---- result types -----------------------------------------------------------
@dataclass
class Finding:
    kind: str                 # skew | tiny_tasks | small_files | spill | gc | python_udf | broadcast_candidate
    severity: str             # high | medium | low
    stage_id: Optional[int]
    title: str
    evidence: List[str]
    fix: str


@dataclass
class StageSummary:
    stage_id: int
    name: str
    n_tasks: int
    duration_ms: float
    share_of_window: float
    on_critical_path: bool
    buckets_ms: Dict[str, float]      # task-time per bucket (summed over tasks)
    bucket_share: Dict[str, float]
    binding_resource: str
    input_bytes: int
    shuffle_read_bytes: int
    shuffle_write_bytes: int
    spill_disk_bytes: int
    spill_mem_bytes: int
    task_run_ms_median: float
    task_run_ms_max: float


@dataclass
class Diagnosis:
    app_name: str
    observed_slots: int
    target_slots: int
    window_ms: float
    covered_ms: float
    gap_ms: float
    critical_path: List[int]
    stages: List[StageSummary]
    dominant_stage: Optional[int]
    verdict: str
    projected_ms: float = 0.0   # SIMULATED wall-clock at target_slots (see diagnose())
    findings: List[Finding] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


# ---- stage analysis ---------------------------------------------------------
def _bucket_times(stage: Stage) -> Dict[str, float]:
    cpu = gc = fetch = overhead = other = 0.0
    for t in stage.tasks:
        c = min(t.cpu_ms, t.run_ms)
        g = min(t.gc_ms, max(0.0, t.run_ms - c))
        f = min(t.sh_fetch_wait_ms, max(0.0, t.run_ms - c - g))
        o = max(0.0, t.run_ms - c - g - f)
        cpu += c
        gc += g
        fetch += f
        other += o
        overhead += t.deser_ms + t.ser_ms + t.getting_result_ms + t.sched_delay_ms
    return {"cpu": cpu, "gc": gc, "fetch_wait": fetch, "overhead": overhead, "other_wait": other}


def _binding_resource(shares: Dict[str, float], flags: set) -> str:
    """Dominant bucket, refined by what the evidence says the 'other' wait is."""
    top = max(shares, key=shares.get)
    if top == "cpu":
        return "CPU-bound (JVM compute)"
    if top == "gc":
        return "Memory pressure (GC-bound)"
    if top == "fetch_wait":
        return "Shuffle-fetch bound (network / remote disk)"
    if top == "overhead":
        return "Scheduling / per-task overhead bound"
    # other_wait: say *what* we are waiting on if the evidence allows
    if "python_udf" in flags:
        return "Waiting on Python workers (UDF serialisation + interpreter)"
    if "spill" in flags:
        return "Spill-to-disk bound (memory too small for the data per task)"
    if "small_files" in flags:
        return "File open / metadata bound (too many small files)"
    return "I/O wait (storage or network, cause not identified from the log)"


def _stage_label(stage: Stage) -> str:
    """Spark's own stage names are just call sites. Derive what the stage DOES from its metrics."""
    acc = stage.accumulables
    parts = []
    if "scan time" in acc or any(t.in_bytes for t in stage.tasks):
        parts.append("scan")
    if any(t.sh_read_bytes for t in stage.tasks) or "fetch wait time" in acc:
        parts.append("shuffle-read")
    if "sort time" in acc:
        parts.append("sort")
    if "time in aggregation build" in acc or "avg hash probes per key" in acc:
        parts.append("hash-agg")
    if any(t.sh_write_bytes for t in stage.tasks):
        parts.append("shuffle-write")
    site = stage.name.split("\n")[0]
    if " at " in site:
        verb, loc = site.split(" at ", 1)
        site = f"{verb}@{loc.rsplit('/', 1)[-1]}"
    return (" > ".join(parts) if parts else "compute") + f"  [{site[:34]}]"


def _stage_summary(stage: Stage, window_ms: float, on_cp: bool, flags: set) -> StageSummary:
    b = _bucket_times(stage)
    total = sum(b.values()) or 1.0
    share = {k: v / total for k, v in b.items()}
    runs = [t.run_ms for t in stage.tasks]
    return StageSummary(
        stage_id=stage.id,
        name=_stage_label(stage),
        n_tasks=len(stage.tasks),
        duration_ms=stage.duration_ms,
        share_of_window=stage.duration_ms / window_ms if window_ms else 0.0,
        on_critical_path=on_cp,
        buckets_ms=b,
        bucket_share=share,
        binding_resource=_binding_resource(share, flags),
        input_bytes=sum(t.in_bytes for t in stage.tasks),
        shuffle_read_bytes=sum(t.sh_read_bytes for t in stage.tasks),
        shuffle_write_bytes=sum(t.sh_write_bytes for t in stage.tasks),
        spill_disk_bytes=sum(t.spill_disk for t in stage.tasks),
        spill_mem_bytes=sum(t.spill_mem for t in stage.tasks),
        task_run_ms_median=_median(runs),
        task_run_ms_max=max(runs) if runs else 0.0,
    )


# ---- pathology rules --------------------------------------------------------
def _rule_skew(stage: Stage, target_slots: int) -> Optional[Finding]:
    if len(stage.tasks) < 8:
        return None
    ordered = sorted(stage.tasks, key=lambda t: t.launch)
    runs = [t.run_ms for t in ordered]
    med, mx = _median(runs), max(runs)
    if med <= 0 or mx < SKEW_MIN_MAX_MS or mx / med < SKEW_RATIO:
        return None
    worst = max(ordered, key=lambda t: t.run_ms)
    reads = [t.sh_read_bytes + t.in_bytes for t in ordered]
    # A time imbalance NOT explained by a data imbalance is JIT warm-up / a slow node,
    # not data skew. Require the data to be lopsided too (when we can see data sizes).
    if max(reads) > 0 and _median(reads) > 0 and max(reads) / _median(reads) < SKEW_MIN_DATA_RATIO:
        return None
    ev = [
        f"Task {worst.index} ran {mx:,.0f} ms vs stage median {med:,.0f} ms "
        f"({mx / med:.1f}x median, {mx / sum(runs):.0%} of all task time in the stage).",
    ]
    if max(reads) > 0 and _median(reads) > 0:
        ev.append(
            f"Input bytes: max task {_fmt_bytes(max(reads))} vs median "
            f"{_fmt_bytes(_median(reads))} -> the imbalance is in the DATA, not the node."
        )
    durs = [t.duration_ms for t in ordered]
    makespan = greedy_makespan(durs, target_slots)
    balanced = sum(durs) / target_slots
    tax = makespan - balanced
    ev.append(
        f"MODEL ESTIMATE, UNVALIDATED (replays measured task durations; under-predicted real 4-core "
        f"times by 38-60% in this repo's validation, so use for direction only) on {target_slots} slots: "
        f"stage takes {makespan:,.0f} ms; perfectly balanced it would take {balanced:,.0f} ms "
        f"-> straggler tax {tax:,.0f} ms ({tax / makespan:.0%} of stage time)."
        if makespan > 0 else "Projection unavailable."
    )
    sev = "high" if makespan and tax / makespan >= 0.3 else "medium"
    return Finding(
        "skew", sev, stage.id,
        "Data skew: one task dominates the stage",
        ev,
        "Find the hot key(s). Options in order of preference: (1) broadcast the small side so "
        "the hot key never shuffles; (2) enable AQE skew-join (spark.sql.adaptive.skewJoin.enabled) "
        "and tune skewedPartitionThresholdInBytes; (3) salt the hot key(s) on the big side and "
        "replicate the small side; (4) pre-aggregate before the join.",
    )


def _rule_tiny_tasks(stage: Stage, overhead_share: float, target_slots: int) -> Optional[Finding]:
    n = len(stage.tasks)
    if n <= 4 * target_slots:
        return None
    durs = [t.duration_ms for t in stage.tasks]
    med = _median(durs)
    if med >= TINY_TASK_MEDIAN_MS or overhead_share < TINY_TASK_OVERHEAD_SHARE:
        return None
    total_bytes = sum(t.sh_read_bytes + t.in_bytes for t in stage.tasks)
    per_task = total_bytes / n if n else 0
    ideal = max(target_slots * 2, int(total_bytes / (128 * 1024 * 1024)) + 1)
    return Finding(
        "tiny_tasks", "high" if overhead_share >= 0.3 else "medium", stage.id,
        "Too many tiny tasks: fixed per-task cost dominates",
        [
            f"{n} tasks, median duration {med:,.0f} ms, ~{_fmt_bytes(per_task)} of data per task.",
            f"Per-task overhead (deserialise + scheduler delay + result serialisation) is "
            f"{overhead_share:.0%} of all task time in this stage.",
        ],
        f"Reduce partition count to roughly {ideal} (target 100-200 MiB per task, at least "
        f"2-3x the slot count). Set spark.sql.shuffle.partitions lower, or enable AQE "
        f"partition coalescing (spark.sql.adaptive.coalescePartitions.enabled).",
    )


def _rule_small_files(app: AppLog, job_ids: set) -> Optional[Finding]:
    worst = None
    for job in app.jobs.values():
        if job.id not in job_ids or job.sql_execution_id is None:
            continue
        ex = app.sql.get(job.sql_execution_id)
        for sc in (ex.scan_nodes() if ex else []):
            n, size = sc["files"], sc["bytes"]
            if n >= SMALL_FILE_MIN_COUNT and size / n < SMALL_FILE_AVG_BYTES:
                if worst is None or n > worst["files"]:
                    worst = sc
    if not worst:
        return None
    n, size = worst["files"], worst["bytes"]
    ev = [f"Scan read {int(n):,} files totalling {_fmt_bytes(size)} "
          f"-> average file size {_fmt_bytes(size / n)} (healthy is 128 MiB to 1 GiB)."]
    if worst.get("metadata_ms"):
        ev.append(f"{worst['metadata_ms']:,.0f} ms spent listing files / reading metadata on the driver.")
    return Finding(
        "small_files", "high", None,
        "Small-files problem: per-file list/open/footer cost dominates",
        ev,
        "Compact into 128 MiB - 1 GiB files (OPTIMIZE on Delta, or a scheduled rewrite job). "
        "Fix it at WRITE time (coalesce/repartition before write, or use auto-compaction). "
        "Compaction has a one-off cost: it pays back only if the data is read often enough.",
    )


def _rule_spill(stage: Stage) -> Optional[Finding]:
    disk = sum(t.spill_disk for t in stage.tasks)
    mem = sum(t.spill_mem for t in stage.tasks)
    if disk <= 0 and mem <= 0:
        return None
    n_spilled = sum(1 for t in stage.tasks if t.spill_disk > 0)
    return Finding(
        "spill", "high" if disk > 512 * 1024 * 1024 else "medium", stage.id,
        "Spill: tasks did not fit their data in execution memory",
        [
            f"{_fmt_bytes(mem)} spilled from memory ({_fmt_bytes(disk)} on disk, compressed), "
            f"in {n_spilled} of {len(stage.tasks)} tasks.",
            "Spilled data is written, then read back and merged: extra disk I/O plus CPU.",
        ],
        "Give each task less data (more partitions / fix skew), or more memory per task "
        "(fewer cores per executor, larger executor memory / spark.memory.fraction). "
        "Prefer the first: it is free.",
    )


def _rule_gc(stage: Stage, gc_share: float) -> Optional[Finding]:
    if gc_share < GC_FLAG_SHARE:
        return None
    return Finding(
        "gc", "high" if gc_share >= 0.3 else "medium", stage.id,
        "High GC time",
        [f"JVM garbage collection is {gc_share:.0%} of task time in this stage."],
        "Look for large per-task object graphs (collect_list, wide rows, UDF boxing). "
        "Reduce rows per task, avoid caching deserialised objects, consider off-heap.",
    )


def _rule_python_udf(app: AppLog, stage_ids: set) -> Optional[Finding]:
    markers = ("BatchEvalPython", "ArrowEvalPython", "FlatMapGroupsInPandas", "MapInPandas",
               "FlatMapCoGroupsInPandas", "AggregateInPandas")
    for job in app.jobs.values():
        if job.sql_execution_id is None or not (set(job.stage_ids) & stage_ids):
            continue
        plan = app.sql.get(job.sql_execution_id)
        if not plan:
            continue
        for line in plan.plan.splitlines():
            for m in markers:
                if m in line:
                    return Finding(
                        "python_udf", "high", None,
                        "Python UDF in the physical plan: rows cross the JVM/Python boundary",
                        [f"Plan operator: {line.strip()[:110]}",
                         "Every row is serialised to a Python worker and back; the JVM thread "
                         "shows up as 'other waiting', not CPU. The optimizer also cannot "
                         "push filters or prune columns through the UDF."],
                        "Replace with built-in Spark SQL functions (F.when, F.regexp_*, F.expr). "
                        "If logic truly needs Python, use a pandas UDF (Arrow, vectorised) "
                        "instead of a row-at-a-time UDF.",
                    )
    return None


def _rule_broadcast_candidate(app: AppLog, stages: List[Stage]) -> Optional[Finding]:
    has_smj = any("SortMergeJoin" in s.plan for s in app.sql.values())
    if not has_smj:
        return None
    scan_writers = [s for s in stages if not s.parents and sum(t.sh_write_bytes for t in s.tasks) > 0]
    if len(scan_writers) < 2:
        return None
    sizes = sorted((sum(t.sh_write_bytes for t in s.tasks), s.id) for s in scan_writers)
    small, big = sizes[0], sizes[-1]
    if small[0] <= 0 or big[0] / small[0] < BROADCAST_RATIO or small[0] > BROADCAST_SMALL_BYTES:
        return None
    return Finding(
        "broadcast_candidate", "medium", big[1],
        "Sort-merge join with a small side that was shuffled",
        [f"Join input shuffle sizes: stage {small[1]} wrote {_fmt_bytes(small[0])}, "
         f"stage {big[1]} wrote {_fmt_bytes(big[0])} ({big[0] / small[0]:.0f}x larger).",
         "Both sides were hash-partitioned and sorted across the network although one side "
         f"would fit in a broadcast (< {_fmt_bytes(BROADCAST_SMALL_BYTES)})."],
        "Broadcast the small side (F.broadcast(df) or raise spark.sql.autoBroadcastJoinThreshold). "
        "This removes the shuffle of the big side entirely, and with it any join-key skew.",
    )


# ---- critical path ----------------------------------------------------------
def critical_path(stages: Dict[int, Stage]) -> List[int]:
    ran = {sid for sid, s in stages.items() if s.complete is not None and s.tasks}
    memo: Dict[int, tuple] = {}

    def best(sid: int):
        if sid in memo:
            return memo[sid]
        s = stages[sid]
        parents = [p for p in s.parents if p in ran]
        base = max((best(p) for p in parents), key=lambda x: x[0], default=(0.0, []))
        memo[sid] = (base[0] + s.duration_ms, base[1] + [sid])
        return memo[sid]

    if not ran:
        return []
    return max((best(s) for s in ran), key=lambda x: x[0])[1]


def _union_ms(intervals: List[tuple]) -> float:
    total, cur_s, cur_e = 0.0, None, None
    for s, e in sorted(intervals):
        if cur_e is None or s > cur_e:
            if cur_e is not None:
                total += cur_e - cur_s
            cur_s, cur_e = s, e
        else:
            cur_e = max(cur_e, e)
    if cur_e is not None:
        total += cur_e - cur_s
    return total


# ---- entry point ------------------------------------------------------------
def diagnose(app: AppLog, target_slots: int = 16,
             window_ms: Optional[tuple] = None) -> Diagnosis:
    """window_ms=(start_epoch_ms, end_epoch_ms) restricts analysis to jobs that ran inside it
    (e.g. to exclude warm-up or checksum jobs). Default: every job in the log."""
    jobs = [j for j in app.jobs.values() if j.complete is not None]
    if window_ms:
        lo, hi = window_ms
        jobs = [j for j in jobs if j.submit >= lo - 50 and j.complete <= hi + 50]
    job_stage_ids = {sid for j in jobs for sid in j.stage_ids}
    ran = {sid: s for sid, s in app.stages.items()
           if s.complete is not None and s.tasks and sid in job_stage_ids}
    if not ran or not jobs:
        return Diagnosis(app.app_name, app.observed_slots, target_slots, 0, 0, 0, [], [], None,
                         "No completed stages found in log.")
    w_start = min(j.submit for j in jobs)
    w_end = max(j.complete for j in jobs)
    window = float(w_end - w_start)
    covered = _union_ms([(s.submit, s.complete) for s in ran.values()])
    cp = critical_path({sid: s for sid, s in app.stages.items() if sid in ran})
    cp_set = set(cp)

    # per-stage pathology rules (evidence), computed before binding resource so the
    # 'other wait' can be refined by what we found
    all_stage_ids = set(ran)
    findings: List[Finding] = []
    stage_flags: Dict[int, set] = {sid: set() for sid in ran}
    udf = _rule_python_udf(app, all_stage_ids)
    sf = _rule_small_files(app, {j.id for j in jobs})

    for sid, s in ran.items():
        share = s.duration_ms / window if window else 0
        if sid not in cp_set and share < MIN_STAGE_SHARE:
            continue
        b = _bucket_times(s)
        total = sum(b.values()) or 1.0
        for rule_finding in (
            _rule_skew(s, target_slots),
            _rule_tiny_tasks(s, b["overhead"] / total, target_slots),
            _rule_spill(s),
            _rule_gc(s, b["gc"] / total),
        ):
            if rule_finding:
                findings.append(rule_finding)
                stage_flags[sid].add(rule_finding.kind)
    if sf:
        findings.append(sf)
        for sid in ran:
            stage_flags[sid].add("small_files")
    if udf:
        findings.append(udf)
        for sid in ran:
            stage_flags[sid].add("python_udf")
    bc = _rule_broadcast_candidate(app, list(ran.values()))
    if bc:
        findings.append(bc)

    summaries = [_stage_summary(s, window, sid in cp_set, stage_flags[sid]) for sid, s in sorted(ran.items())]
    cp_summaries = [x for x in summaries if x.on_critical_path]
    dominant = max(cp_summaries, key=lambda x: x.duration_ms) if cp_summaries else None

    sev_rank = {"high": 0, "medium": 1, "low": 2}
    findings.sort(key=lambda f: sev_rank[f.severity])

    verdict = (f"Stage {dominant.stage_id} dominates the critical path "
               f"({dominant.share_of_window:.0%} of wall-clock); binding resource: "
               f"{dominant.binding_resource}.") if dominant else "No critical path."
    # NOTE: this projection FAILED validation against real 4-core runs (README, "Model validation").
    # It is reported but never used to decide anything. Original description:
    # Projection: replay each critical-path stage's MEASURED task durations through a greedy
    # scheduler with `target_slots` slots, then add the measured non-stage gap. This is a model,
    # not a measurement: it ignores shuffle-network contention and assumes tasks don't slow
    # down when run in parallel. It is only used to show the cost of imbalance (skew).
    projected = sum(
        greedy_makespan([t.duration_ms for t in sorted(ran[sid].tasks, key=lambda t: t.launch)], target_slots)
        for sid in cp) + max(0.0, window - covered)
    return Diagnosis(
        app_name=app.app_name, observed_slots=app.observed_slots, target_slots=target_slots,
        window_ms=window, covered_ms=covered, gap_ms=max(0.0, window - covered),
        critical_path=cp, stages=summaries,
        dominant_stage=dominant.stage_id if dominant else None,
        verdict=verdict, projected_ms=projected, findings=findings,
    )


# ---- reporting --------------------------------------------------------------
def render_markdown(d: Diagnosis) -> str:
    out: List[str] = []
    out.append(f"### Diagnosis: `{d.app_name}`")
    out.append(f"*Observed slots: {d.observed_slots}. Projections use {d.target_slots} slots.*\n")
    out.append("**Step 0 - wall-clock window**  ")
    out.append(f"{d.window_ms / 1000:.1f} s total; {d.covered_ms / 1000:.1f} s inside stages, "
               f"{d.gap_ms / 1000:.1f} s ({(d.gap_ms / d.window_ms if d.window_ms else 0):.0%}) "
               f"outside any stage (driver planning, listing, collect).\n")
    out.append("**Step 1 - WHERE: critical path**\n")
    out.append("| stage | name | tasks | duration | % of window | on critical path |")
    out.append("|---:|---|---:|---:|---:|:--:|")
    for s in sorted(d.stages, key=lambda x: -x.duration_ms)[:6]:
        out.append(f"| {s.stage_id} | {s.name} | {s.n_tasks} | {s.duration_ms / 1000:.2f} s | "
                   f"{s.share_of_window:.0%} | {'yes' if s.on_critical_path else ''} |")
    dom = next((s for s in d.stages if s.stage_id == d.dominant_stage), None)
    if dom:
        out.append(f"\n**Step 2 - WHAT KIND: where the dominant stage ({dom.stage_id}) spends task time**\n")
        out.append("| bucket | task-seconds | share |")
        out.append("|---|---:|---:|")
        for k in BUCKETS:
            out.append(f"| {BUCKET_MEANING[k]} | {dom.buckets_ms[k] / 1000:.2f} | {dom.bucket_share[k]:.0%} |")
        out.append(f"\n**Binding resource:** {dom.binding_resource}\n")
    out.append("**Step 3 - WHY: evidence-backed findings**\n")
    if not d.findings:
        out.append("No pathology rule fired. The stage is doing honest work.\n")
    for i, f in enumerate(d.findings, 1):
        loc = f" (stage {f.stage_id})" if f.stage_id is not None else ""
        out.append(f"{i}. **[{f.severity.upper()}] {f.title}**{loc}")
        for e in f.evidence:
            out.append(f"   - {e}")
        out.append(f"   - *Fix:* {f.fix}")
    return "\n".join(out) + "\n"
