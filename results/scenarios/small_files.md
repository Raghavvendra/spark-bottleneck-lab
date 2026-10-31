# small_files

Scan 1,500 tiny parquet files vs. the same rows compacted to 6 files.

## Diagnosis of the BAD run (what the tool says, from the event log only)

### Diagnosis: `small_files:bad`
*Observed slots: 4. Projections use 16 slots.*

**Step 0 - wall-clock window**  
2.8 s total; 2.3 s inside stages, 0.5 s (16%) outside any stage (driver planning, listing, collect).

**Step 1 - WHERE: critical path**

| stage | name | tasks | duration | % of window | on critical path |
|---:|---|---:|---:|---:|:--:|
| 4 | scan > hash-agg > shuffle-write  [collect@scenarios.py:110] | 47 | 2.20 s | 79% | yes |
| 5 | shuffle-read > hash-agg  [collect@scenarios.py:110] | 8 | 0.10 s | 4% | yes |
| 3 | compute  [parquet@NativeMethodAccessorImpl.j] | 1 | 0.04 s | 1% |  |

**Step 2 - WHAT KIND: where the dominant stage (4) spends task time**

| bucket | task-seconds | share |
|---|---:|---:|
| JVM compute (executorCpuTime) | 6.16 | 71% |
| Garbage collection pauses | 0.24 | 3% |
| Waiting for shuffle blocks (network / remote disk) | 0.00 | 0% |
| Per-task overhead (deserialise + scheduler delay + result ser.) | 0.56 | 6% |
| Other waiting inside the task (storage I/O, spill I/O, local shuffle reads, Python workers) | 1.75 | 20% |

**Binding resource:** CPU-bound (JVM compute)

**Step 3 - WHY: evidence-backed findings**

1. **[HIGH] Small-files problem: per-file list/open/footer cost dominates**
   - Scan read 1,500 files totalling 32.9 MiB -> average file size 22.5 KiB (healthy is 128 MiB to 1 GiB).
   - 6 ms spent listing files / reading metadata on the driver.
   - *Fix:* Compact into 128 MiB - 1 GiB files (OPTIMIZE on Delta, or a scheduled rewrite job). Fix it at WRITE time (coalesce/repartition before write, or use auto-compaction). Compaction has a one-off cost: it pays back only if the data is read often enough.

## Re-measure: `compacted`

- wall-clock (median of 3): **3.06s -> 1.14s** (2.68x)
- model estimate on 16 slots (UNVALIDATED, ignore for conclusions): 1.21s -> 0.98s (1.24x)
- result checksum identical: **True**
- binding resource: CPU-bound (JVM compute)  ->  CPU-bound (JVM compute)
- findings resolved: ['small_files']; still present: none; newly introduced: none
- **verdict:** CONFIRMED: 2.68x faster measured, result identical; the same resource still binds, with less work.

<details><summary>Diagnosis of this variant</summary>

### Diagnosis: `small_files:compacted`
*Observed slots: 4. Projections use 16 slots.*

**Step 0 - wall-clock window**  
1.1 s total; 0.6 s inside stages, 0.4 s (39%) outside any stage (driver planning, listing, collect).

**Step 1 - WHERE: critical path**

| stage | name | tasks | duration | % of window | on critical path |
|---:|---|---:|---:|---:|:--:|
| 4 | scan > hash-agg > shuffle-write  [collect@scenarios.py:110] | 3 | 0.52 s | 49% | yes |
| 5 | shuffle-read > hash-agg  [collect@scenarios.py:110] | 8 | 0.10 s | 9% | yes |
| 3 | compute  [parquet@NativeMethodAccessorImpl.j] | 1 | 0.03 s | 3% |  |

**Step 2 - WHAT KIND: where the dominant stage (4) spends task time**

| bucket | task-seconds | share |
|---|---:|---:|
| JVM compute (executorCpuTime) | 0.84 | 55% |
| Garbage collection pauses | 0.07 | 4% |
| Waiting for shuffle blocks (network / remote disk) | 0.00 | 0% |
| Per-task overhead (deserialise + scheduler delay + result ser.) | 0.04 | 3% |
| Other waiting inside the task (storage I/O, spill I/O, local shuffle reads, Python workers) | 0.57 | 38% |

**Binding resource:** CPU-bound (JVM compute)

**Step 3 - WHY: evidence-backed findings**

No pathology rule fired. The stage is doing honest work.


</details>
