# over_partitioned

groupBy with spark.sql.shuffle.partitions=3000 on a ~1 GB-class input.

## Diagnosis of the BAD run (what the tool says, from the event log only)

### Diagnosis: `over_partitioned:bad`
*Observed slots: 4. Projections use 16 slots.*

**Step 0 - wall-clock window**  
6.9 s total; 6.5 s inside stages, 0.4 s (6%) outside any stage (driver planning, listing, collect).

**Step 1 - WHERE: critical path**

| stage | name | tasks | duration | % of window | on critical path |
|---:|---|---:|---:|---:|:--:|
| 5 | shuffle-read > hash-agg  [collect@scenarios.py:110] | 3000 | 5.73 s | 83% | yes |
| 4 | scan > hash-agg > shuffle-write  [collect@scenarios.py:110] | 6 | 0.74 s | 11% | yes |
| 3 | compute  [parquet@NativeMethodAccessorImpl.j] | 1 | 0.04 s | 1% |  |

**Step 2 - WHAT KIND: where the dominant stage (5) spends task time**

| bucket | task-seconds | share |
|---|---:|---:|
| JVM compute (executorCpuTime) | 6.54 | 26% |
| Garbage collection pauses | 0.69 | 3% |
| Waiting for shuffle blocks (network / remote disk) | 0.00 | 0% |
| Per-task overhead (deserialise + scheduler delay + result ser.) | 17.66 | 70% |
| Other waiting inside the task (storage I/O, spill I/O, local shuffle reads, Python workers) | 0.29 | 1% |

**Binding resource:** Scheduling / per-task overhead bound

**Step 3 - WHY: evidence-backed findings**

1. **[HIGH] Too many tiny tasks: fixed per-task cost dominates** (stage 5)
   - 3000 tasks, median duration 6 ms, ~125 B of data per task.
   - Per-task overhead (deserialise + scheduler delay + result serialisation) is 70% of all task time in this stage.
   - *Fix:* Reduce partition count to roughly 32 (target 100-200 MiB per task, at least 2-3x the slot count). Set spark.sql.shuffle.partitions lower, or enable AQE partition coalescing (spark.sql.adaptive.coalescePartitions.enabled).

## Re-measure: `tuned`

- wall-clock (median of 3): **7.02s -> 1.19s** (5.92x)
- model estimate on 16 slots (UNVALIDATED, ignore for conclusions): 2.62s -> 0.91s (2.88x)
- result checksum identical: **True**
- binding resource: Scheduling / per-task overhead bound  ->  CPU-bound (JVM compute)
- findings resolved: ['tiny_tasks']; still present: none; newly introduced: none
- **verdict:** CONFIRMED: 5.92x faster measured, result identical; the binding resource changed.

<details><summary>Diagnosis of this variant</summary>

### Diagnosis: `over_partitioned:tuned`
*Observed slots: 4. Projections use 16 slots.*

**Step 0 - wall-clock window**  
1.1 s total; 0.7 s inside stages, 0.4 s (33%) outside any stage (driver planning, listing, collect).

**Step 1 - WHERE: critical path**

| stage | name | tasks | duration | % of window | on critical path |
|---:|---|---:|---:|---:|:--:|
| 4 | scan > hash-agg > shuffle-write  [collect@scenarios.py:110] | 6 | 0.59 s | 54% | yes |
| 5 | shuffle-read > hash-agg  [collect@scenarios.py:110] | 8 | 0.10 s | 9% | yes |
| 3 | compute  [parquet@NativeMethodAccessorImpl.j] | 1 | 0.04 s | 4% |  |

**Step 2 - WHAT KIND: where the dominant stage (4) spends task time**

| bucket | task-seconds | share |
|---|---:|---:|
| JVM compute (executorCpuTime) | 1.13 | 53% |
| Garbage collection pauses | 0.10 | 4% |
| Waiting for shuffle blocks (network / remote disk) | 0.00 | 0% |
| Per-task overhead (deserialise + scheduler delay + result ser.) | 0.12 | 6% |
| Other waiting inside the task (storage I/O, spill I/O, local shuffle reads, Python workers) | 0.80 | 37% |

**Binding resource:** CPU-bound (JVM compute)

**Step 3 - WHY: evidence-backed findings**

No pathology rule fired. The stage is doing honest work.


</details>

## Re-measure: `aqe`

- wall-clock (median of 3): **7.02s -> 1.35s** (5.18x)
- model estimate on 16 slots (UNVALIDATED, ignore for conclusions): 2.62s -> 0.99s (2.64x)
- result checksum identical: **True**
- binding resource: Scheduling / per-task overhead bound  ->  CPU-bound (JVM compute)
- findings resolved: ['tiny_tasks']; still present: none; newly introduced: none
- **verdict:** CONFIRMED: 5.18x faster measured, result identical; the binding resource changed.

<details><summary>Diagnosis of this variant</summary>

### Diagnosis: `over_partitioned:aqe`
*Observed slots: 4. Projections use 16 slots.*

**Step 0 - wall-clock window**  
1.3 s total; 0.9 s inside stages, 0.4 s (31%) outside any stage (driver planning, listing, collect).

**Step 1 - WHERE: critical path**

| stage | name | tasks | duration | % of window | on critical path |
|---:|---|---:|---:|---:|:--:|
| 5 | scan > hash-agg > shuffle-write  [collect@scenarios.py:110] | 6 | 0.73 s | 57% | yes |
| 7 | shuffle-read > hash-agg  [collect@scenarios.py:110] | 1 | 0.10 s | 8% |  |
| 4 | compute  [parquet@NativeMethodAccessorImpl.j] | 1 | 0.04 s | 3% |  |

**Step 2 - WHAT KIND: where the dominant stage (5) spends task time**

| bucket | task-seconds | share |
|---|---:|---:|
| JVM compute (executorCpuTime) | 1.46 | 56% |
| Garbage collection pauses | 0.10 | 4% |
| Waiting for shuffle blocks (network / remote disk) | 0.00 | 0% |
| Per-task overhead (deserialise + scheduler delay + result ser.) | 0.11 | 4% |
| Other waiting inside the task (storage I/O, spill I/O, local shuffle reads, Python workers) | 0.94 | 36% |

**Binding resource:** CPU-bound (JVM compute)

**Step 3 - WHY: evidence-backed findings**

No pathology rule fired. The stage is doing honest work.


</details>
