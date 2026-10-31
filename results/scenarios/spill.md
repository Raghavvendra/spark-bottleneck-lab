# spill

RED HERRING: real spill in a 2-partition sort, but the sort is not the critical path.

## Diagnosis of the BAD run (what the tool says, from the event log only)

### Diagnosis: `spill:bad`
*Observed slots: 4. Projections use 16 slots.*

**Step 0 - wall-clock window**  
6.2 s total; 5.8 s inside stages, 0.4 s (7%) outside any stage (driver planning, listing, collect).

**Step 1 - WHERE: critical path**

| stage | name | tasks | duration | % of window | on critical path |
|---:|---|---:|---:|---:|:--:|
| 4 | scan > shuffle-write  [save@NativeMethodAccessorImpl.java] | 6 | 3.01 s | 49% | yes |
| 5 | shuffle-read > sort  [save@NativeMethodAccessorImpl.java] | 2 | 2.60 s | 42% | yes |
| 6 | scan > hash-agg > shuffle-write  [collect@scenarios.py:193] | 6 | 0.10 s | 2% |  |
| 3 | compute  [parquet@NativeMethodAccessorImpl.j] | 1 | 0.05 s | 1% |  |
| 7 | shuffle-read > hash-agg  [collect@scenarios.py:193] | 1 | 0.03 s | 0% |  |

**Step 2 - WHAT KIND: where the dominant stage (4) spends task time**

| bucket | task-seconds | share |
|---|---:|---:|
| JVM compute (executorCpuTime) | 8.86 | 88% |
| Garbage collection pauses | 0.26 | 3% |
| Waiting for shuffle blocks (network / remote disk) | 0.00 | 0% |
| Per-task overhead (deserialise + scheduler delay + result ser.) | 0.07 | 1% |
| Other waiting inside the task (storage I/O, spill I/O, local shuffle reads, Python workers) | 0.84 | 8% |

**Binding resource:** CPU-bound (JVM compute)

**Step 3 - WHY: evidence-backed findings**

1. **[MEDIUM] Spill: tasks did not fit their data in execution memory** (stage 5)
   - 368.0 MiB spilled from memory (263.9 MiB on disk, compressed), in 2 of 2 tasks.
   - Spilled data is written, then read back and merged: extra disk I/O plus CPU.
   - *Fix:* Give each task less data (more partitions / fix skew), or more memory per task (fewer cores per executor, larger executor memory / spark.memory.fraction). Prefer the first: it is free.
2. **[MEDIUM] Spill: tasks did not fit their data in execution memory** (stage 4)
   - 284.0 MiB spilled from memory (212.4 MiB on disk, compressed), in 6 of 6 tasks.
   - Spilled data is written, then read back and merged: extra disk I/O plus CPU.
   - *Fix:* Give each task less data (more partitions / fix skew), or more memory per task (fewer cores per executor, larger executor memory / spark.memory.fraction). Prefer the first: it is free.

## Re-measure: `more_partitions`

- wall-clock (median of 3): **6.28s -> 4.89s** (1.28x)
- model estimate on 16 slots (UNVALIDATED, ignore for conclusions): 5.03s -> 2.80s (1.80x)
- result checksum identical: **True**
- binding resource: CPU-bound (JVM compute)  ->  CPU-bound (JVM compute)
- findings resolved: none; still present: ['spill']; newly introduced: none
- **verdict:** CONFIRMED: 1.28x faster measured, result identical; the same resource still binds, with less work.

<details><summary>Diagnosis of this variant</summary>

### Diagnosis: `spill:more_partitions`
*Observed slots: 4. Projections use 16 slots.*

**Step 0 - wall-clock window**  
4.8 s total; 4.4 s inside stages, 0.4 s (9%) outside any stage (driver planning, listing, collect).

**Step 1 - WHERE: critical path**

| stage | name | tasks | duration | % of window | on critical path |
|---:|---|---:|---:|---:|:--:|
| 4 | scan > shuffle-write  [save@NativeMethodAccessorImpl.java] | 6 | 2.96 s | 61% | yes |
| 5 | shuffle-read > sort  [save@NativeMethodAccessorImpl.java] | 24 | 1.27 s | 26% | yes |
| 6 | scan > hash-agg > shuffle-write  [collect@scenarios.py:193] | 6 | 0.08 s | 2% |  |
| 3 | compute  [parquet@NativeMethodAccessorImpl.j] | 1 | 0.05 s | 1% |  |
| 7 | shuffle-read > hash-agg  [collect@scenarios.py:193] | 1 | 0.03 s | 1% |  |

**Step 2 - WHAT KIND: where the dominant stage (4) spends task time**

| bucket | task-seconds | share |
|---|---:|---:|
| JVM compute (executorCpuTime) | 8.63 | 88% |
| Garbage collection pauses | 0.23 | 2% |
| Waiting for shuffle blocks (network / remote disk) | 0.00 | 0% |
| Per-task overhead (deserialise + scheduler delay + result ser.) | 0.09 | 1% |
| Other waiting inside the task (storage I/O, spill I/O, local shuffle reads, Python workers) | 0.85 | 9% |

**Binding resource:** CPU-bound (JVM compute)

**Step 3 - WHY: evidence-backed findings**

1. **[MEDIUM] Spill: tasks did not fit their data in execution memory** (stage 4)
   - 284.0 MiB spilled from memory (212.4 MiB on disk, compressed), in 6 of 6 tasks.
   - Spilled data is written, then read back and merged: extra disk I/O plus CPU.
   - *Fix:* Give each task less data (more partitions / fix skew), or more memory per task (fewer cores per executor, larger executor memory / spark.memory.fraction). Prefer the first: it is free.

</details>
