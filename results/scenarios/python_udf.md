# python_udf

Row-at-a-time Python UDF vs. pandas UDF vs. built-in expressions.

## Diagnosis of the BAD run (what the tool says, from the event log only)

### Diagnosis: `python_udf:bad`
*Observed slots: 4. Projections use 16 slots.*

**Step 0 - wall-clock window**  
2.2 s total; 1.7 s inside stages, 0.5 s (24%) outside any stage (driver planning, listing, collect).

**Step 1 - WHERE: critical path**

| stage | name | tasks | duration | % of window | on critical path |
|---:|---|---:|---:|---:|:--:|
| 4 | scan > hash-agg > shuffle-write  [collect@scenarios.py:110] | 6 | 1.56 s | 71% | yes |
| 5 | shuffle-read > hash-agg  [collect@scenarios.py:110] | 8 | 0.07 s | 3% | yes |
| 3 | compute  [parquet@NativeMethodAccessorImpl.j] | 1 | 0.04 s | 2% |  |

**Step 2 - WHAT KIND: where the dominant stage (4) spends task time**

| bucket | task-seconds | share |
|---|---:|---:|
| JVM compute (executorCpuTime) | 1.07 | 19% |
| Garbage collection pauses | 0.13 | 2% |
| Waiting for shuffle blocks (network / remote disk) | 0.00 | 0% |
| Per-task overhead (deserialise + scheduler delay + result ser.) | 0.13 | 2% |
| Other waiting inside the task (storage I/O, spill I/O, local shuffle reads, Python workers) | 4.25 | 76% |

**Binding resource:** Waiting on Python workers (UDF serialisation + interpreter)

**Step 3 - WHY: evidence-backed findings**

1. **[HIGH] Python UDF in the physical plan: rows cross the JVM/Python boundary**
   - Plan operator: +- BatchEvalPython (5)
   - Every row is serialised to a Python worker and back; the JVM thread shows up as 'other waiting', not CPU. The optimizer also cannot push filters or prune columns through the UDF.
   - *Fix:* Replace with built-in Spark SQL functions (F.when, F.regexp_*, F.expr). If logic truly needs Python, use a pandas UDF (Arrow, vectorised) instead of a row-at-a-time UDF.

## Re-measure: `pandas_udf`

- wall-clock (median of 3): **2.26s -> 2.82s** (0.80x)
- model estimate on 16 slots (UNVALIDATED, ignore for conclusions): 1.83s -> 2.44s (0.75x)
- result checksum identical: **True**
- binding resource: Waiting on Python workers (UDF serialisation + interpreter)  ->  Waiting on Python workers (UDF serialisation + interpreter)
- findings resolved: none; still present: ['python_udf']; newly introduced: none
- **verdict:** REGRESSION: 1.25x SLOWER measured. The fix costs more than the problem at this scale.

<details><summary>Diagnosis of this variant</summary>

### Diagnosis: `python_udf:pandas_udf`
*Observed slots: 4. Projections use 16 slots.*

**Step 0 - wall-clock window**  
2.8 s total; 2.0 s inside stages, 0.8 s (29%) outside any stage (driver planning, listing, collect).

**Step 1 - WHERE: critical path**

| stage | name | tasks | duration | % of window | on critical path |
|---:|---|---:|---:|---:|:--:|
| 4 | scan > hash-agg > shuffle-write  [collect@scenarios.py:110] | 6 | 1.82 s | 66% | yes |
| 5 | shuffle-read > hash-agg  [collect@scenarios.py:110] | 8 | 0.10 s | 3% | yes |
| 3 | compute  [parquet@NativeMethodAccessorImpl.j] | 1 | 0.04 s | 2% |  |

**Step 2 - WHAT KIND: where the dominant stage (4) spends task time**

| bucket | task-seconds | share |
|---|---:|---:|
| JVM compute (executorCpuTime) | 1.04 | 15% |
| Garbage collection pauses | 0.12 | 2% |
| Waiting for shuffle blocks (network / remote disk) | 0.00 | 0% |
| Per-task overhead (deserialise + scheduler delay + result ser.) | 0.12 | 2% |
| Other waiting inside the task (storage I/O, spill I/O, local shuffle reads, Python workers) | 5.48 | 81% |

**Binding resource:** Waiting on Python workers (UDF serialisation + interpreter)

**Step 3 - WHY: evidence-backed findings**

1. **[HIGH] Python UDF in the physical plan: rows cross the JVM/Python boundary**
   - Plan operator: +- ArrowEvalPython (5)
   - Every row is serialised to a Python worker and back; the JVM thread shows up as 'other waiting', not CPU. The optimizer also cannot push filters or prune columns through the UDF.
   - *Fix:* Replace with built-in Spark SQL functions (F.when, F.regexp_*, F.expr). If logic truly needs Python, use a pandas UDF (Arrow, vectorised) instead of a row-at-a-time UDF.

</details>

## Re-measure: `native`

- wall-clock (median of 3): **2.26s -> 1.30s** (1.73x)
- model estimate on 16 slots (UNVALIDATED, ignore for conclusions): 1.83s -> 1.06s (1.73x)
- result checksum identical: **True**
- binding resource: Waiting on Python workers (UDF serialisation + interpreter)  ->  CPU-bound (JVM compute)
- findings resolved: ['python_udf']; still present: none; newly introduced: none
- **verdict:** CONFIRMED: 1.73x faster measured, result identical; the binding resource changed.

<details><summary>Diagnosis of this variant</summary>

### Diagnosis: `python_udf:native`
*Observed slots: 4. Projections use 16 slots.*

**Step 0 - wall-clock window**  
1.2 s total; 0.8 s inside stages, 0.5 s (39%) outside any stage (driver planning, listing, collect).

**Step 1 - WHERE: critical path**

| stage | name | tasks | duration | % of window | on critical path |
|---:|---|---:|---:|---:|:--:|
| 4 | scan > hash-agg > shuffle-write  [collect@scenarios.py:110] | 6 | 0.65 s | 53% | yes |
| 5 | shuffle-read > hash-agg  [collect@scenarios.py:110] | 8 | 0.07 s | 6% | yes |
| 3 | compute  [parquet@NativeMethodAccessorImpl.j] | 1 | 0.04 s | 3% |  |

**Step 2 - WHAT KIND: where the dominant stage (4) spends task time**

| bucket | task-seconds | share |
|---|---:|---:|
| JVM compute (executorCpuTime) | 1.16 | 49% |
| Garbage collection pauses | 0.10 | 4% |
| Waiting for shuffle blocks (network / remote disk) | 0.00 | 0% |
| Per-task overhead (deserialise + scheduler delay + result ser.) | 0.12 | 5% |
| Other waiting inside the task (storage I/O, spill I/O, local shuffle reads, Python workers) | 0.97 | 41% |

**Binding resource:** CPU-bound (JVM compute)

**Step 3 - WHY: evidence-backed findings**

No pathology rule fired. The stage is doing honest work.


</details>
