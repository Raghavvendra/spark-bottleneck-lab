# Playbook: diagnosing a slow Spark job

Numbers below come from this repo's measured runs (`results/`). Read the README limitations first.

## 1. The approach and why it is ordered this way

**Where, then what kind, then why, then fix, then re-measure.**

| Order | Why this order |
|---|---|
| Where (critical path) | Optimising anything off the critical path buys nothing (Amdahl). Skipping this is how you "fix" spill by 1%. |
| What kind (time buckets) | Tells you which *resource* binds: CPU, GC, waiting, or overhead. The fix family follows from it. |
| Why (pathology rules) | Only now name a cause, with evidence. A cause named before a measurement is a guess. |
| Fix + re-measure | The bottleneck moves. If you don't re-measure you don't know you fixed the right thing. |

"Why" is not secondary, it is *third*: the cause without the location fixes the wrong stage; the location without the cause fixes it wrongly.

## 2. Beginner vs expert, on the same job

| Situation | Beginner | Expert |
|---|---|---|
| Job is slow | Guesses (skew? shuffle?), adds compute | Compares to baseline, decomposes wall-clock, finds the critical-path stage |
| Sees one slow task | "Skew!" | Checks whether the *data* is lopsided too; time-only imbalance is JIT or a bad node |
| Sees spill | Fixes spill | Asks whether the spilling stage is on the critical path first (here the fix gained a modest 1.28x) |
| 3,000 tiny tasks | Blames the cluster | Reads the per-task overhead share, lowers partitions or lets AQE coalesce |
| Python UDF job | Tunes executors | Sees JVM threads *waiting*, replaces the UDF |
| Fix applied | "Looks faster" | Verifies identical result, same-or-better wall-clock, and that the diagnosed cause is gone |

## 3. Tradeoffs worth knowing cold

- **Salting vs AQE skew-join.** Salting costs extra passes and per-row work and complicates code; AQE splits hot partitions at runtime. Measured on 4 cores: salting 0.91x (slower), AQE 1.33x. Reach for salting when AQE can't split (e.g. aggregations with non-combinable functions, or skew in a non-join stage).
- **Compaction vs reading small files.** Compaction is a one-off write cost that pays back per read. Fix at write time; compacting after the fact is the expensive route.
- **Partition count.** Too many: overhead dominates (5.9x slower here). Too few: no parallelism and spill. Target roughly 100-200 MiB per task and at least 2-3x the slots.
- **pandas UDF vs native.** Vectorisation is not automatically cheaper: here the pandas UDF (2.82 s) was slower than the plain UDF (2.26 s); only native expressions (1.30 s) helped. Native expressions also let the optimizer see through the logic.
- **Broadcast vs shuffle join.** Broadcast removes the big side's shuffle *and* its key skew, but only while the small side really fits driver and executor memory.

## 4. Pitfalls (including ones I fell into building this)

1. **Trusting a confident explanation that arrived before a measurement.** My first diagnoser flagged skew on a cold-JIT task.
2. **Staging a win.** My first "fixed" skew variant was slower than the baseline. The honest move is to report it.
3. **Measuring one run.** Run-to-run variation can be large on shared machines; use medians of fresh JVMs.
4. **Trusting an unvalidated model.** My scheduler projection under-predicted real 4-core times by 38-60% and ranked the wrong fix best. Validate a model against measurement before using it, and drop it if it fails.
5. **Version drift.** Spark 3.5 + Java 21 + Arrow crashes; pandas 3 breaks PySpark 3.5. Pin versions.
6. **Fixing and not checking the answer.** Every variant here returns a checksum; a fast wrong answer is a regression.

## 5. Transferable meta-patterns

- **Theory of constraints:** locate the constraint, classify it, explain it, relieve it, re-measure. Works for pipelines, queries, data-quality incidents, a food stall's queue.
- **Distinguish a symptom from a cause from the thing that matters.** Spill (symptom) vs skew (cause) vs critical path (what matters).
- **Require corroboration from an independent signal** before naming a cause (time *and* data size for skew).
- **Every optimisation is a hypothesis with a falsifier:** result identical, clock moved, diagnosed cause gone.
- **Report negative results.** They are what make the positive ones believable.

## 6. Interview questions this project lets you answer with evidence

- "A Spark job got 3x slower overnight. Walk me through it." -> window, critical path, bucket breakdown, what changed.
- "When would you not use salting?" -> measured regression; AQE first.
- "Why is the task count important?" -> thousands of tiny tasks where per-task overhead dominates.
- "How do you know your fix worked?" -> checksum, median-of-3, cause disappeared from the diagnosis.
- "What are the limits of your numbers?" -> local mode only, a projection model I validated and dropped, synthetic data, self-graded detection.
