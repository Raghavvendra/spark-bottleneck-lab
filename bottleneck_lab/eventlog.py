"""Parse a Spark event log (JSON lines) into typed objects.

Works on ANY Spark 3.x event log (local, YARN, K8s, Databricks-exported),
not just the ones produced by this lab. Only the standard listener events
are used, so there is no dependency on a running Spark UI / History Server.
"""
from __future__ import annotations

import gzip
import json
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class Task:
    stage_id: int
    index: int
    launch: int            # epoch ms
    finish: int            # epoch ms
    run_ms: float          # executorRunTime
    cpu_ms: float          # executorCpuTime (ns in log -> ms here)
    gc_ms: float
    deser_ms: float
    ser_ms: float
    getting_result_ms: float
    spill_mem: int
    spill_disk: int
    sh_read_bytes: int
    sh_fetch_wait_ms: float
    sh_write_bytes: int
    sh_write_ms: float     # ns in log -> ms here
    in_bytes: int
    in_records: int
    failed: bool

    @property
    def duration_ms(self) -> float:
        return float(self.finish - self.launch)

    @property
    def sched_delay_ms(self) -> float:
        """Same definition the Spark UI uses for 'Scheduler Delay'."""
        d = self.duration_ms - self.run_ms - self.deser_ms - self.ser_ms - self.getting_result_ms
        return max(0.0, d)


@dataclass
class Stage:
    id: int
    attempt: int
    name: str
    n_tasks: int
    parents: List[int]
    submit: Optional[int] = None
    complete: Optional[int] = None
    accumulables: Dict[str, str] = field(default_factory=dict)
    tasks: List[Task] = field(default_factory=list)

    @property
    def duration_ms(self) -> float:
        if self.submit is None or self.complete is None:
            return 0.0
        return float(self.complete - self.submit)


@dataclass
class Job:
    id: int
    submit: int
    complete: Optional[int]
    stage_ids: List[int]
    sql_execution_id: Optional[int]


@dataclass
class SqlExecution:
    id: int
    description: str
    plan: str
    start: int
    end: Optional[int] = None
    # accumulatorId -> (node name, metric name); filled from sparkPlanInfo
    metric_ids: Dict[int, tuple] = field(default_factory=dict)
    # (node name, metric name) -> value, from SparkListenerDriverAccumUpdates
    driver_metrics: Dict[tuple, float] = field(default_factory=dict)

    def scan_nodes(self) -> List[dict]:
        """Per file-scan node: files read, bytes read, metadata time (driver-side metrics)."""
        nodes: Dict[str, dict] = {}
        for (node, metric), v in self.driver_metrics.items():
            if not node.startswith(("Scan", "FileScan", "BatchScan")):
                continue
            d = nodes.setdefault(node, {"node": node})
            if metric == "number of files read":
                d["files"] = v
            elif metric == "size of files read":
                d["bytes"] = v
            elif metric == "metadata time":
                d["metadata_ms"] = v
        return [d for d in nodes.values() if "files" in d and "bytes" in d]


@dataclass
class AppLog:
    app_name: str = ""
    spark_conf: Dict[str, str] = field(default_factory=dict)
    start: Optional[int] = None
    end: Optional[int] = None
    jobs: Dict[int, Job] = field(default_factory=dict)
    stages: Dict[int, Stage] = field(default_factory=dict)
    sql: Dict[int, SqlExecution] = field(default_factory=dict)

    @property
    def observed_slots(self) -> int:
        """Parallel task slots actually available (best effort)."""
        master = self.spark_conf.get("spark.master", "")
        if master.startswith("local["):
            inner = master[len("local["):-1]
            if inner.isdigit():
                return int(inner)
            if inner.startswith("*"):
                return os.cpu_count() or 1
        if master == "local":
            return 1
        try:
            instances = int(self.spark_conf.get("spark.executor.instances", "1"))
            cores = int(self.spark_conf.get("spark.executor.cores", "1"))
            return max(1, instances * cores)
        except ValueError:
            return 1


def _collect_metric_ids(node: dict, out: Dict[int, tuple]) -> None:
    name = node.get("nodeName", "")
    for m in node.get("metrics", []) or []:
        out[m["accumulatorId"]] = (name, m["name"])
    for c in node.get("children", []) or []:
        _collect_metric_ids(c, out)


def _open(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, "r", encoding="utf-8")


def _task_from_event(e: dict) -> Optional[Task]:
    tm = e.get("Task Metrics")
    ti = e.get("Task Info")
    if not tm or not ti:
        return None  # failed tasks can lack metrics
    sr = tm.get("Shuffle Read Metrics", {})
    sw = tm.get("Shuffle Write Metrics", {})
    im = tm.get("Input Metrics", {})
    return Task(
        stage_id=e["Stage ID"],
        index=ti["Index"],
        launch=ti["Launch Time"],
        finish=ti["Finish Time"],
        run_ms=tm.get("Executor Run Time", 0),
        cpu_ms=tm.get("Executor CPU Time", 0) / 1e6,
        gc_ms=tm.get("JVM GC Time", 0),
        deser_ms=tm.get("Executor Deserialize Time", 0),
        ser_ms=tm.get("Result Serialization Time", 0),
        getting_result_ms=ti.get("Getting Result Time", 0),
        spill_mem=tm.get("Memory Bytes Spilled", 0),
        spill_disk=tm.get("Disk Bytes Spilled", 0),
        sh_read_bytes=sr.get("Remote Bytes Read", 0) + sr.get("Local Bytes Read", 0),
        sh_fetch_wait_ms=sr.get("Fetch Wait Time", 0),
        sh_write_bytes=sw.get("Shuffle Bytes Written", 0),
        sh_write_ms=sw.get("Shuffle Write Time", 0) / 1e6,
        in_bytes=im.get("Bytes Read", 0),
        in_records=im.get("Records Read", 0),
        failed=bool(ti.get("Failed", False)),
    )


def parse_event_log(path: str) -> AppLog:
    app = AppLog()
    with _open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue  # truncated last line of a still-running app
            ev = e.get("Event", "")
            if ev == "SparkListenerApplicationStart":
                app.app_name = e.get("App Name", "")
                app.start = e.get("Timestamp")
            elif ev == "SparkListenerApplicationEnd":
                app.end = e.get("Timestamp")
            elif ev == "SparkListenerEnvironmentUpdate":
                app.spark_conf = dict(e.get("Spark Properties", {}))
            elif ev == "SparkListenerJobStart":
                props = e.get("Properties", {}) or {}
                sql_id = props.get("spark.sql.execution.id")
                app.jobs[e["Job ID"]] = Job(
                    id=e["Job ID"],
                    submit=e["Submission Time"],
                    complete=None,
                    stage_ids=list(e.get("Stage IDs", [])),
                    sql_execution_id=int(sql_id) if sql_id is not None else None,
                )
                for si in e.get("Stage Infos", []):
                    sid = si["Stage ID"]
                    if sid not in app.stages:
                        app.stages[sid] = Stage(
                            id=sid,
                            attempt=si.get("Stage Attempt ID", 0),
                            name=si.get("Stage Name", ""),
                            n_tasks=si.get("Number of Tasks", 0),
                            parents=list(si.get("Parent IDs", [])),
                        )
            elif ev == "SparkListenerJobEnd":
                j = app.jobs.get(e["Job ID"])
                if j:
                    j.complete = e.get("Completion Time")
            elif ev == "SparkListenerStageSubmitted":
                si = e["Stage Info"]
                st = app.stages.setdefault(
                    si["Stage ID"],
                    Stage(si["Stage ID"], si.get("Stage Attempt ID", 0), si.get("Stage Name", ""),
                          si.get("Number of Tasks", 0), list(si.get("Parent IDs", []))),
                )
                st.submit = si.get("Submission Time")
            elif ev == "SparkListenerStageCompleted":
                si = e["Stage Info"]
                st = app.stages.setdefault(
                    si["Stage ID"],
                    Stage(si["Stage ID"], si.get("Stage Attempt ID", 0), si.get("Stage Name", ""),
                          si.get("Number of Tasks", 0), list(si.get("Parent IDs", []))),
                )
                st.submit = si.get("Submission Time", st.submit)
                st.complete = si.get("Completion Time")
                st.accumulables = {a["Name"]: a.get("Value") for a in si.get("Accumulables", [])
                                   if "Name" in a}
            elif ev == "SparkListenerTaskEnd":
                t = _task_from_event(e)
                if t is not None and not t.failed:
                    st = app.stages.get(t.stage_id)
                    if st is not None:
                        st.tasks.append(t)
            elif ev.endswith("SparkListenerSQLExecutionStart"):
                ex = SqlExecution(
                    id=e["executionId"],
                    description=e.get("description", ""),
                    plan=e.get("physicalPlanDescription", ""),
                    start=e.get("time", 0),
                )
                _collect_metric_ids(e.get("sparkPlanInfo", {}) or {}, ex.metric_ids)
                app.sql[e["executionId"]] = ex
            elif ev.endswith("SparkListenerDriverAccumUpdates"):
                ex = app.sql.get(e.get("executionId"))
                if ex:
                    for acc_id, val in e.get("accumUpdates", []):
                        key = ex.metric_ids.get(acc_id)
                        if key:
                            ex.driver_metrics[key] = ex.driver_metrics.get(key, 0) + val
            elif ev.endswith("SparkListenerSQLExecutionEnd"):
                s = app.sql.get(e["executionId"])
                if s:
                    s.end = e.get("time")
    return app
