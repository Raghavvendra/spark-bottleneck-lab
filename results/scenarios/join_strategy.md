# join_strategy

Dimension join forced to sort-merge vs. broadcast hash join.

## Diagnosis of the BAD run (what the tool says, from the event log only)

### Diagnosis: `join_strategy:bad`
*Observed slots: 4. Projections use 16 slots.*

**Step 0 - wall-clock window**  
2.8 s total; 2.1 s inside stages, 0.7 s (24%) outside any stage (driver planning, listing, collect).

**Step 1 - WHERE: critical path**

| stage | name | tasks | duration | % of window | on critical path |
|---:|---|---:|---:|---:|:--:|
| 7 | shuffle-read > sort > hash-agg > shuffle-write  [collect@scenarios.py:110] | 8 | 1.24 s | 44% | yes |
| 5 | scan > shuffle-write  [collect@scenarios.py:110] | 6 | 0.75 s | 27% | yes |
| 6 | scan > shuffle-write  [collect@scenarios.py:110] | 1 | 0.62 s | 22% |  |
| 8 | shuffle-read > hash-agg  [collect@scenarios.py:110] | 8 | 0.07 s | 2% | yes |
| 4 | compute  [parquet@NativeMethodAccessorImpl.j] | 1 | 0.04 s | 2% |  |
| 3 | compute  [parquet@NativeMethodAccessorImpl.j] | 1 | 0.04 s | 1% |  |

**Step 2 - WHAT KIND: where the dominant stage (7) spends task time**

| bucket | task-seconds | share |
|---|---:|---:|
| JVM compute (executorCpuTime) | 3.80 | 77% |
| Garbage collection pauses | 0.14 | 3% |
| Waiting for shuffle blocks (network / remote disk) | 0.00 | 0% |
| Per-task overhead (deserialise + scheduler delay + result ser.) | 0.14 | 3% |
| Other waiting inside the task (storage I/O, spill I/O, local shuffle reads, Python workers) | 0.83 | 17% |

**Binding resource:** CPU-bound (JVM compute)

**Step 3 - WHY: evidence-backed findings**

1. **[MEDIUM] Sort-merge join with a small side that was shuffled** (stage 5)
   - Join input shuffle sizes: stage 6 wrote 666.8 KiB, stage 5 wrote 56.5 MiB (87x larger).
   - Both sides were hash-partitioned and sorted across the network although one side would fit in a broadcast (< 128.0 MiB).
   - *Fix:* Broadcast the small side (F.broadcast(df) or raise spark.sql.autoBroadcastJoinThreshold). This removes the shuffle of the big side entirely, and with it any join-key skew.

## Re-measure: `broadcast`

- wall-clock (median of 3): **2.87s -> 1.96s** (1.47x)
- model estimate on 16 slots (UNVALIDATED, ignore for conclusions): 2.15s -> 1.29s (1.66x)
- result checksum identical: **True**
- binding resource: CPU-bound (JVM compute)  ->  CPU-bound (JVM compute)
- findings resolved: ['broadcast_candidate']; still present: none; newly introduced: none
- **verdict:** CONFIRMED: 1.47x faster measured, result identical; the same resource still binds, with less work.

<details><summary>Diagnosis of this variant</summary>

### Diagnosis: `join_strategy:broadcast`
*Observed slots: 4. Projections use 16 slots.*

**Step 0 - wall-clock window**  
1.9 s total; 1.3 s inside stages, 0.6 s (33%) outside any stage (driver planning, listing, collect).

**Step 1 - WHERE: critical path**

| stage | name | tasks | duration | % of window | on critical path |
|---:|---|---:|---:|---:|:--:|
| 6 | scan > hash-agg > shuffle-write  [collect@scenarios.py:110] | 6 | 0.84 s | 44% | yes |
| 5 | scan  [collect@scenarios.py:110] | 1 | 0.26 s | 14% |  |
| 7 | shuffle-read > hash-agg  [collect@scenarios.py:110] | 8 | 0.07 s | 4% | yes |
| 3 | compute  [parquet@NativeMethodAccessorImpl.j] | 1 | 0.05 s | 3% |  |
| 4 | compute  [parquet@NativeMethodAccessorImpl.j] | 1 | 0.04 s | 2% |  |

**Step 2 - WHAT KIND: where the dominant stage (6) spends task time**

| bucket | task-seconds | share |
|---|---:|---:|
| JVM compute (executorCpuTime) | 2.24 | 78% |
| Garbage collection pauses | 0.10 | 4% |
| Waiting for shuffle blocks (network / remote disk) | 0.00 | 0% |
| Per-task overhead (deserialise + scheduler delay + result ser.) | 0.12 | 4% |
| Other waiting inside the task (storage I/O, spill I/O, local shuffle reads, Python workers) | 0.43 | 15% |

**Binding resource:** CPU-bound (JVM compute)

**Step 3 - WHY: evidence-backed findings**

No pathology rule fired. The stage is doing honest work.


</details>
