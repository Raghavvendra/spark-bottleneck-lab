"""Each rule is tested on a hand-built stage whose pathology is known by construction,
plus a NEGATIVE test (healthy stage -> no finding). A diagnoser that cries wolf is
worse than none: people stop reading it.
"""
import json
import os
import tempfile

import pytest

from bottleneck_lab.diagnose import (diagnose, greedy_makespan, critical_path,
                                     _rule_skew, _rule_tiny_tasks, _rule_spill, _bucket_times,
                                     _binding_resource)
from bottleneck_lab.eventlog import AppLog, Job, Stage, Task, SqlExecution, parse_event_log


def task(i, stage=0, start=0, run=1000, cpu=900, gc=0, deser=5, in_b=0, sh_r=0, sh_w=0,
         fetch=0, spill_d=0, spill_m=0, dur=None):
    dur = dur if dur is not None else run + deser + 2
    return Task(stage_id=stage, index=i, launch=start, finish=start + dur, run_ms=run, cpu_ms=cpu,
                gc_ms=gc, deser_ms=deser, ser_ms=1, getting_result_ms=0, spill_mem=spill_m,
                spill_disk=spill_d, sh_read_bytes=sh_r, sh_fetch_wait_ms=fetch, sh_write_bytes=sh_w,
                sh_write_ms=0, in_bytes=in_b, in_records=0, failed=False)


def stage(sid, tasks, parents=(), t0=0, t1=None):
    t1 = t1 if t1 is not None else t0 + sum(t.duration_ms for t in tasks)
    return Stage(id=sid, attempt=0, name=f"stage {sid}", n_tasks=len(tasks),
                 parents=list(parents), submit=t0, complete=t1, tasks=tasks)


# ---------------------------------------------------------------- scheduling model
def test_makespan_balanced_vs_skewed():
    assert greedy_makespan([100] * 16, 16) == 100
    assert greedy_makespan([100] * 15 + [1600], 16) == 1600   # one straggler gates the stage
    assert greedy_makespan([100] * 32, 16) == 200
    assert greedy_makespan([], 4) == 0


# ---------------------------------------------------------------- skew
def test_skew_detected_when_time_and_data_are_lopsided():
    ts = [task(i, run=500, sh_r=2_000_000) for i in range(15)] + \
         [task(15, run=8000, sh_r=40_000_000)]
    f = _rule_skew(stage(1, ts), target_slots=16)
    assert f is not None and f.kind == "skew" and f.severity == "high"
    assert "straggler tax" in " ".join(f.evidence)


def test_skew_NOT_flagged_when_only_time_is_lopsided():
    """Cold JIT / slow node: first task slow but data sizes equal -> not data skew."""
    ts = [task(0, run=3000, sh_r=2_000_000)] + [task(i, run=400, sh_r=2_000_000) for i in range(1, 16)]
    assert _rule_skew(stage(1, ts), target_slots=16) is None


def test_skew_NOT_flagged_on_subsecond_noise():
    ts = [task(i, run=20, sh_r=1000) for i in range(15)] + [task(15, run=200, sh_r=9000)]
    assert _rule_skew(stage(1, ts), target_slots=16) is None


def test_skew_NOT_flagged_on_healthy_stage():
    ts = [task(i, run=1000 + 30 * (i % 3), sh_r=5_000_000) for i in range(32)]
    assert _rule_skew(stage(1, ts), target_slots=16) is None


# ---------------------------------------------------------------- tiny tasks
def test_tiny_tasks_detected():
    ts = [task(i, run=8, cpu=6, deser=6, dur=30, sh_r=20_000) for i in range(3000)]
    s = stage(2, ts)
    b = _bucket_times(s)
    share = b["overhead"] / sum(b.values())
    f = _rule_tiny_tasks(s, share, target_slots=16)
    assert f is not None and f.kind == "tiny_tasks"


def test_tiny_tasks_NOT_flagged_for_few_big_tasks():
    ts = [task(i, run=5000) for i in range(8)]
    s = stage(2, ts)
    assert _rule_tiny_tasks(s, 0.01, target_slots=16) is None


# ---------------------------------------------------------------- spill
def test_spill_detected_and_absent():
    assert _rule_spill(stage(3, [task(0, spill_d=300_000_000, spill_m=900_000_000)])).kind == "spill"
    assert _rule_spill(stage(3, [task(0)])) is None


# ---------------------------------------------------------------- binding resource
def test_binding_resource_refines_other_wait_with_evidence():
    shares = {"cpu": 0.1, "gc": 0.0, "fetch_wait": 0.0, "overhead": 0.05, "other_wait": 0.85}
    assert "Python" in _binding_resource(shares, {"python_udf"})
    assert "Spill" in _binding_resource(shares, {"spill"})
    assert "small files" in _binding_resource(shares, {"small_files"})
    assert "CPU" in _binding_resource({"cpu": .8, "gc": .05, "fetch_wait": 0, "overhead": .05, "other_wait": .1}, set())
    assert "GC" in _binding_resource({"cpu": .3, "gc": .5, "fetch_wait": 0, "overhead": .05, "other_wait": .15}, set())


# ---------------------------------------------------------------- critical path
def test_critical_path_ignores_short_parallel_branch():
    a = stage(0, [task(0, run=9000)], t0=0, t1=9000)          # long scan
    b = stage(1, [task(0, run=500)], t0=0, t1=500)            # short parallel scan
    c = stage(2, [task(0, run=2000)], parents=[0, 1], t0=9000, t1=11000)
    assert critical_path({0: a, 1: b, 2: c}) == [0, 2]


# ---------------------------------------------------------------- end to end on a synthetic app
def test_end_to_end_skewed_app_names_skew_as_first_finding():
    app = AppLog(app_name="synthetic", spark_conf={"spark.master": "local[1]"})
    scan = stage(0, [task(i, stage=0, in_b=64_000_000, sh_w=2_000_000, run=900) for i in range(4)], t0=0, t1=3600)
    ts = [task(i, stage=1, start=3600 + i * 600, run=500, sh_r=2_000_000) for i in range(15)] + \
         [task(15, stage=1, start=3600 + 15 * 600, run=9000, sh_r=50_000_000)]
    red = stage(1, ts, parents=[0], t0=3600, t1=3600 + 15 * 600 + 9000 + 10)
    app.stages = {0: scan, 1: red}
    app.jobs = {0: Job(0, 0, red.complete, [0, 1], None)}
    d = diagnose(app, target_slots=16)
    assert d.dominant_stage == 1
    assert d.findings and d.findings[0].kind == "skew"
    assert d.projected_ms < (d.window_ms)    # parallelism would help


# ---------------------------------------------------------------- parser robustness
def test_parser_survives_truncated_log_and_unknown_events():
    lines = [
        {"Event": "SparkListenerApplicationStart", "App Name": "x", "Timestamp": 1},
        {"Event": "SomethingNew", "foo": 1},
        {"Event": "SparkListenerJobStart", "Job ID": 0, "Submission Time": 10, "Stage IDs": [0],
         "Stage Infos": [{"Stage ID": 0, "Stage Attempt ID": 0, "Stage Name": "s", "Number of Tasks": 1,
                          "Parent IDs": []}], "Properties": {}},
    ]
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        for l in lines:
            fh.write(json.dumps(l) + "\n")
        fh.write('{"Event": "SparkListenerTaskEnd", "Stage ID"')   # truncated mid-write
        path = fh.name
    try:
        app = parse_event_log(path)
        assert app.app_name == "x" and 0 in app.stages and 0 in app.jobs
    finally:
        os.unlink(path)
