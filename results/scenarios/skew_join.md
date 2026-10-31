# skew_join

Sort-merge join on a skewed key (50% of rows share one key); the hot partition spills.

## Diagnosis of the BAD run (what the tool says, from the event log only)

### Diagnosis: `skew_join:bad`
*Observed slots: 4. Projections use 16 slots.*

**Step 0 - wall-clock window**  
6.4 s total; 5.8 s inside stages, 0.6 s (10%) outside any stage (driver planning, listing, collect).

**Step 1 - WHERE: critical path**

| stage | name | tasks | duration | % of window | on critical path |
|---:|---|---:|---:|---:|:--:|
| 7 | shuffle-read > sort > shuffle-write  [collect@scenarios.py:110] | 16 | 4.06 s | 64% | yes |
| 6 | scan > shuffle-write  [collect@scenarios.py:110] | 6 | 1.50 s | 23% | yes |
| 5 | scan > shuffle-write  [collect@scenarios.py:110] | 1 | 0.39 s | 6% |  |
| 8 | shuffle-read > sort  [collect@scenarios.py:110] | 16 | 0.13 s | 2% | yes |
| 3 | compute  [parquet@NativeMethodAccessorImpl.j] | 1 | 0.04 s | 1% |  |
| 4 | compute  [parquet@NativeMethodAccessorImpl.j] | 1 | 0.03 s | 1% |  |

**Step 2 - WHAT KIND: where the dominant stage (7) spends task time**

| bucket | task-seconds | share |
|---|---:|---:|
| JVM compute (executorCpuTime) | 7.27 | 84% |
| Garbage collection pauses | 0.27 | 3% |
| Waiting for shuffle blocks (network / remote disk) | 0.00 | 0% |
| Per-task overhead (deserialise + scheduler delay + result ser.) | 0.27 | 3% |
| Other waiting inside the task (storage I/O, spill I/O, local shuffle reads, Python workers) | 0.90 | 10% |

**Binding resource:** CPU-bound (JVM compute)

**Step 3 - WHY: evidence-backed findings**

1. **[HIGH] Data skew: one task dominates the stage** (stage 7)
   - Task 13 ran 2,740 ms vs stage median 290 ms (9.4x median, 32% of all task time in the stage).
   - Input bytes: max task 118.4 MiB vs median 7.7 MiB -> the imbalance is in the DATA, not the node.
   - MODEL ESTIMATE, UNVALIDATED (replays measured task durations; under-predicted real 4-core times by 38-60% in this repo's validation, so use for direction only) on 16 slots: stage takes 2,751 ms; perfectly balanced it would take 544 ms -> straggler tax 2,207 ms (80% of stage time).
   - *Fix:* Find the hot key(s). Options in order of preference: (1) broadcast the small side so the hot key never shuffles; (2) enable AQE skew-join (spark.sql.adaptive.skewJoin.enabled) and tune skewedPartitionThresholdInBytes; (3) salt the hot key(s) on the big side and replicate the small side; (4) pre-aggregate before the join.
2. **[MEDIUM] Spill: tasks did not fit their data in execution memory** (stage 7)
   - 785.0 MiB spilled from memory (260.7 MiB on disk, compressed), in 16 of 16 tasks.
   - Spilled data is written, then read back and merged: extra disk I/O plus CPU.
   - *Fix:* Give each task less data (more partitions / fix skew), or more memory per task (fewer cores per executor, larger executor memory / spark.memory.fraction). Prefer the first: it is free.
3. **[MEDIUM] Sort-merge join with a small side that was shuffled** (stage 6)
   - Join input shuffle sizes: stage 5 wrote 265.9 KiB, stage 6 wrote 232.9 MiB (897x larger).
   - Both sides were hash-partitioned and sorted across the network although one side would fit in a broadcast (< 128.0 MiB).
   - *Fix:* Broadcast the small side (F.broadcast(df) or raise spark.sql.autoBroadcastJoinThreshold). This removes the shuffle of the big side entirely, and with it any join-key skew.

## Re-measure: `salting`

- wall-clock (median of 3): **6.46s -> 7.07s** (0.91x)
- model estimate on 16 slots (UNVALIDATED, ignore for conclusions): 4.42s -> 4.41s (1.00x)
- result checksum identical: **True**
- binding resource: CPU-bound (JVM compute)  ->  CPU-bound (JVM compute)
- findings resolved: ['skew']; still present: ['broadcast_candidate', 'spill']; newly introduced: none
- **verdict:** REGRESSION: 1.09x SLOWER measured. The fix costs more than the problem at this scale.

<details><summary>Diagnosis of this variant</summary>

### Diagnosis: `skew_join:salting`
*Observed slots: 4. Projections use 16 slots.*

**Step 0 - wall-clock window**  
7.0 s total; 5.2 s inside stages, 1.8 s (26%) outside any stage (driver planning, listing, collect).

**Step 1 - WHERE: critical path**

| stage | name | tasks | duration | % of window | on critical path |
|---:|---|---:|---:|---:|:--:|
| 9 | shuffle-read > sort > shuffle-write  [collect@scenarios.py:110] | 16 | 2.80 s | 40% | yes |
| 7 | scan > shuffle-write  [collect@scenarios.py:110] | 6 | 1.44 s | 21% | yes |
| 8 | scan > shuffle-write  [collect@scenarios.py:110] | 1 | 1.03 s | 15% |  |
| 5 | scan > hash-agg > shuffle-write  [collect@scenarios.py:124] | 6 | 0.53 s | 8% |  |
| 6 | shuffle-read > hash-agg  [collect@scenarios.py:124] | 16 | 0.21 s | 3% |  |
| 10 | shuffle-read > sort  [collect@scenarios.py:110] | 16 | 0.14 s | 2% | yes |

**Step 2 - WHAT KIND: where the dominant stage (9) spends task time**

| bucket | task-seconds | share |
|---|---:|---:|
| JVM compute (executorCpuTime) | 9.66 | 91% |
| Garbage collection pauses | 0.27 | 3% |
| Waiting for shuffle blocks (network / remote disk) | 0.00 | 0% |
| Per-task overhead (deserialise + scheduler delay + result ser.) | 0.17 | 2% |
| Other waiting inside the task (storage I/O, spill I/O, local shuffle reads, Python workers) | 0.57 | 5% |

**Binding resource:** CPU-bound (JVM compute)

**Step 3 - WHY: evidence-backed findings**

1. **[MEDIUM] Spill: tasks did not fit their data in execution memory** (stage 9)
   - 779.4 MiB spilled from memory (320.6 MiB on disk, compressed), in 16 of 16 tasks.
   - Spilled data is written, then read back and merged: extra disk I/O plus CPU.
   - *Fix:* Give each task less data (more partitions / fix skew), or more memory per task (fewer cores per executor, larger executor memory / spark.memory.fraction). Prefer the first: it is free.
2. **[MEDIUM] Sort-merge join with a small side that was shuffled** (stage 7)
   - Join input shuffle sizes: stage 8 wrote 277.2 KiB, stage 7 wrote 246.6 MiB (911x larger).
   - Both sides were hash-partitioned and sorted across the network although one side would fit in a broadcast (< 128.0 MiB).
   - *Fix:* Broadcast the small side (F.broadcast(df) or raise spark.sql.autoBroadcastJoinThreshold). This removes the shuffle of the big side entirely, and with it any join-key skew.

</details>

## Re-measure: `aqe`

- wall-clock (median of 3): **6.46s -> 4.87s** (1.33x)
- model estimate on 16 slots (UNVALIDATED, ignore for conclusions): 4.42s -> 1.37s (3.23x)
- result checksum identical: **True**
- binding resource: CPU-bound (JVM compute)  ->  CPU-bound (JVM compute)
- findings resolved: ['skew']; still present: ['broadcast_candidate', 'spill']; newly introduced: none
- **verdict:** CONFIRMED: 1.33x faster measured, result identical; the same resource still binds, with less work.

<details><summary>Diagnosis of this variant</summary>

### Diagnosis: `skew_join:aqe`
*Observed slots: 4. Projections use 16 slots.*

**Step 0 - wall-clock window**  
4.8 s total; 4.2 s inside stages, 0.6 s (13%) outside any stage (driver planning, listing, collect).

**Step 1 - WHERE: critical path**

| stage | name | tasks | duration | % of window | on critical path |
|---:|---|---:|---:|---:|:--:|
| 10 | shuffle-read > sort > shuffle-write  [collect@scenarios.py:110] | 21 | 2.38 s | 49% | yes |
| 6 | scan > shuffle-write  [collect@scenarios.py:110] | 6 | 1.59 s | 33% |  |
| 7 | scan > shuffle-write  [collect@scenarios.py:110] | 1 | 1.12 s | 23% |  |
| 14 | shuffle-read > sort  [collect@scenarios.py:110] | 16 | 0.17 s | 3% |  |
| 4 | compute  [parquet@NativeMethodAccessorImpl.j] | 1 | 0.04 s | 1% |  |
| 5 | compute  [parquet@NativeMethodAccessorImpl.j] | 1 | 0.03 s | 1% |  |

**Step 2 - WHAT KIND: where the dominant stage (10) spends task time**

| bucket | task-seconds | share |
|---|---:|---:|
| JVM compute (executorCpuTime) | 7.56 | 84% |
| Garbage collection pauses | 0.26 | 3% |
| Waiting for shuffle blocks (network / remote disk) | 0.00 | 0% |
| Per-task overhead (deserialise + scheduler delay + result ser.) | 0.34 | 4% |
| Other waiting inside the task (storage I/O, spill I/O, local shuffle reads, Python workers) | 0.81 | 9% |

**Binding resource:** CPU-bound (JVM compute)

**Step 3 - WHY: evidence-backed findings**

1. **[MEDIUM] Spill: tasks did not fit their data in execution memory** (stage 10)
   - 706.2 MiB spilled from memory (227.8 MiB on disk, compressed), in 20 of 21 tasks.
   - Spilled data is written, then read back and merged: extra disk I/O plus CPU.
   - *Fix:* Give each task less data (more partitions / fix skew), or more memory per task (fewer cores per executor, larger executor memory / spark.memory.fraction). Prefer the first: it is free.
2. **[MEDIUM] Sort-merge join with a small side that was shuffled** (stage 6)
   - Join input shuffle sizes: stage 7 wrote 265.9 KiB, stage 6 wrote 232.9 MiB (897x larger).
   - Both sides were hash-partitioned and sorted across the network although one side would fit in a broadcast (< 128.0 MiB).
   - *Fix:* Broadcast the small side (F.broadcast(df) or raise spark.sql.autoBroadcastJoinThreshold). This removes the shuffle of the big side entirely, and with it any join-key skew.

</details>
