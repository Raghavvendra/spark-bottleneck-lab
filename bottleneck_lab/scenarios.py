"""Reproducible Spark workloads that each contain ONE injected bottleneck.

Every scenario has:
  bad    - the pathological version (what a tired engineer ships on Friday)
  fixed  - the manual fix an expert would apply
  aqe    - (where meaningful) the same bad code with Adaptive Query Execution on,
           to show what Spark can and cannot fix for you

Each run executes in its OWN Python process / JVM (clean caches, clean log) and
returns a checksum of the query result, so the harness can prove the fix did not
change the answer.

Usage:
    python -m bottleneck_lab.scenarios setup  --data data --scale 1
    python -m bottleneck_lab.scenarios run --scenario skew_join --variant bad \
        --data data --out results/runs/skew_join_bad_1 --cores 1
"""
import argparse
import glob
import hashlib
import json
import os
import shutil
import sys
import time
from typing import Callable, Dict, List, Optional

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

BASE_ROWS = 6_000_000


# ---------------------------------------------------------------- spark setup
def make_spark(name: str, logdir: Optional[str], cores: int, conf: Dict[str, str],
               driver_mem: str = "1200m") -> SparkSession:
    b = (SparkSession.builder.master(f"local[{cores}]").appName(name)
         .config("spark.driver.memory", driver_mem)
         # Arrow (pandas UDFs) on Java 17+ needs this flag, otherwise: "sun.misc.Unsafe or
         # java.nio.DirectByteBuffer.<init>(long, int) not available"
         .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
         .config("spark.ui.enabled", "false")
         .config("spark.ui.showConsoleProgress", "false")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.parquet.compression.codec", "snappy")
         .config("spark.local.dir", os.environ.get("LAB_TMP", "/tmp/spark-lab-tmp")))
    if logdir:
        os.makedirs(logdir, exist_ok=True)
        b = (b.config("spark.eventLog.enabled", "true")
             .config("spark.eventLog.dir", "file://" + os.path.abspath(logdir))
             .config("spark.eventLog.compress", "false"))
    for k, v in conf.items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def checksum_rows(rows) -> str:
    norm = sorted(tuple(r) for r in rows)
    return hashlib.md5(repr(norm).encode()).hexdigest()[:12]


# ---------------------------------------------------------------- data setup
def setup_data(data: str, scale: float) -> None:
    os.makedirs(data, exist_ok=True)
    rows = int(BASE_ROWS * scale)
    spark = make_spark("setup", None, 1, {"spark.sql.shuffle.partitions": "8",
                                          "spark.sql.adaptive.enabled": "false"})
    t0 = time.time()
    # fact: `key` is heavily skewed (50% of rows on key 0), `g` is uniform.
    fact = (spark.range(rows)
            .withColumn("key", F.when(F.rand(1) < 0.5, F.lit(0))
                        .otherwise((F.rand(2) * 19999).cast("int") + 1))
            .withColumn("g", (F.rand(3) * 50000).cast("int"))
            .withColumn("g1k", F.col("g") % 1000)
            .withColumn("val", (F.rand(4) * 1000).cast("int"))
            .withColumn("s", F.substring(F.md5(F.col("id").cast("string")), 1, 32)))
    fact.repartition(6).write.mode("overwrite").parquet(f"{data}/fact")
    (spark.range(20001).withColumnRenamed("id", "key")
     .withColumn("weight", (F.col("key") % 9 + 1).cast("int"))
     .withColumn("cat", F.concat(F.lit("cat"), (F.col("key") % 50).cast("string")))
     .coalesce(1).write.mode("overwrite").parquet(f"{data}/dim_key"))
    (spark.range(50000).withColumn("g", F.col("id").cast("int")).drop("id")
     .withColumn("weight", (F.col("g") % 9 + 1).cast("int"))
     .withColumn("cat", F.concat(F.lit("cat"), (F.col("g") % 50).cast("string")))
     .coalesce(1).write.mode("overwrite").parquet(f"{data}/dim_g"))

    # small-files copy: same rows, 1500 tiny files; and a compacted copy
    sub = spark.read.parquet(f"{data}/fact").filter(F.col("id") < int(rows * 0.5)) \
        .select("id", "g1k", "val")
    sub.repartition(1500).write.mode("overwrite").parquet(f"{data}/small_files")
    t1 = time.time()
    spark.read.parquet(f"{data}/small_files").coalesce(6) \
        .write.mode("overwrite").parquet(f"{data}/compacted")
    compaction_s = time.time() - t1
    json.dump({"rows": rows, "scale": scale, "compaction_s": compaction_s,
               "setup_s": time.time() - t0}, open(f"{data}/meta.json", "w"))
    spark.stop()


# ---------------------------------------------------------------- scenarios
class Scenario:
    def __init__(self, name: str, what: str, variants: Dict[str, dict],
                 run: Callable, driver_mem: str = "1200m"):
        self.name, self.what, self.variants, self.run, self.driver_mem = name, what, variants, run, driver_mem


def _agg_rows(df):
    return checksum_rows(df.collect())


# 1. skew --------------------------------------------------------------------
def run_skew(spark, data, variant):
    fact = spark.read.parquet(f"{data}/fact")
    dim = spark.read.parquet(f"{data}/dim_key")
    if variant in ("bad", "aqe"):
        j = fact.join(dim, "key")
    else:
        # selective salting: detect hot keys (cost counted in the measurement!)
        K = 16
        # hot keys found by SAMPLING 2% of rows (cheap; this cost is inside the measurement)
        hot = [r["key"] for r in fact.sample(False, 0.02, 11).groupBy("key").count()
               .orderBy(F.desc("count")).limit(5).collect()]
        f2 = fact.withColumn("salt", F.when(F.col("key").isin(hot), (F.rand(7) * K).cast("int")).otherwise(F.lit(0)))
        d2 = (dim.withColumn("salts", F.when(F.col("key").isin(hot), F.sequence(F.lit(0), F.lit(K - 1)))
                             .otherwise(F.array(F.lit(0))))
              .withColumn("salt", F.explode("salts")).drop("salts"))
        j = f2.join(d2, ["key", "salt"])
    out = j.groupBy("cat").agg(F.count("*").alias("n"), F.sum(F.col("val") * F.col("weight")).alias("w"),
                               F.max("s").alias("mx"))
    return _agg_rows(out)


# 2. small files --------------------------------------------------------------
def run_small_files(spark, data, variant):
    path = f"{data}/small_files" if variant == "bad" else f"{data}/compacted"
    df = spark.read.parquet(path)
    out = df.filter(F.col("val") < 500).groupBy("g1k").agg(F.count("*").alias("n"), F.sum("val").alias("s"))
    return _agg_rows(out)


# 3. over-partitioned shuffle ---------------------------------------------------
def run_over_partitioned(spark, data, variant):
    df = spark.read.parquet(f"{data}/fact")
    out = df.groupBy("g1k").agg(F.count("*").alias("n"), F.sum("val").alias("s"))
    return _agg_rows(out)


# 4. python udf -----------------------------------------------------------------
def run_python_udf(spark, data, variant):
    df = spark.read.parquet(f"{data}/fact").filter(F.col("id") < 1_500_000)
    if variant == "bad":
        def band(v):
            m = v % 100
            return "A" if m < 30 else ("B" if m < 80 else "C")
        band_udf = F.udf(band, StringType())
        b = band_udf(F.col("val"))
    elif variant == "pandas_udf":
        import pandas as pd
        from pyspark.sql.functions import pandas_udf

        @pandas_udf(StringType())
        def band_p(v: pd.Series) -> pd.Series:
            m = v % 100
            return m.map(lambda x: "A" if x < 30 else ("B" if x < 80 else "C"))
        b = band_p(F.col("val"))
    else:
        m = F.col("val") % 100
        b = F.when(m < 30, "A").when(m < 80, "B").otherwise("C")
    out = df.withColumn("band", b).groupBy("band").agg(F.count("*").alias("n"), F.sum("val").alias("s"))
    return _agg_rows(out)


# 5. join strategy --------------------------------------------------------------
def run_join_strategy(spark, data, variant):
    fact = spark.read.parquet(f"{data}/fact")
    dim = spark.read.parquet(f"{data}/dim_g")
    if variant == "broadcast":
        dim = F.broadcast(dim)
    j = fact.join(dim, "g")
    out = j.groupBy("cat").agg(F.count("*").alias("n"), F.sum(F.col("val") * F.col("weight")).alias("w"))
    return _agg_rows(out)


# 6. spill ----------------------------------------------------------------------
def run_spill(spark, data, variant):
    P = 2 if variant == "bad" else 24
    df = spark.read.parquet(f"{data}/fact").select("id", "s", "val")
    srt = df.repartition(P).sortWithinPartitions("s")
    srt.write.format("noop").mode("overwrite").save()
    # correctness proxy: the data that went through the sort (not part of timing window logic)
    r = df.agg(F.count("*"), F.sum("val")).collect()[0]
    return checksum_rows([tuple(r)])


NOAQE = {"spark.sql.adaptive.enabled": "false"}
NOBC = {"spark.sql.autoBroadcastJoinThreshold": "-1"}
MEMTIGHT = {"spark.memory.fraction": "0.25", "spark.memory.storageFraction": "0.1"}

SCENARIOS: Dict[str, Scenario] = {
    "skew_join": Scenario(
        "skew_join", "Sort-merge join on a skewed key (50% of rows share one key); the hot partition spills.",
        {"bad": {**NOAQE, **NOBC, **MEMTIGHT, "spark.sql.shuffle.partitions": "16"},
         "salting": {**NOAQE, **NOBC, **MEMTIGHT, "spark.sql.shuffle.partitions": "16"},
         "aqe": {**NOBC, **MEMTIGHT, "spark.sql.shuffle.partitions": "16", "spark.sql.adaptive.enabled": "true",
                 "spark.sql.adaptive.skewJoin.enabled": "true",
                 "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "1m",
                 "spark.sql.adaptive.advisoryPartitionSizeInBytes": "2m",
                 "spark.sql.adaptive.coalescePartitions.enabled": "false"}},
        run_skew, driver_mem="1000m"),
    "small_files": Scenario(
        "small_files", "Scan 1,500 tiny parquet files vs. the same rows compacted to 6 files.",
        {"bad": {**NOAQE, "spark.sql.shuffle.partitions": "8"},
         "compacted": {**NOAQE, "spark.sql.shuffle.partitions": "8"}},
        run_small_files),
    "over_partitioned": Scenario(
        "over_partitioned", "groupBy with spark.sql.shuffle.partitions=3000 on a ~1 GB-class input.",
        {"bad": {**NOAQE, "spark.sql.shuffle.partitions": "3000"},
         "tuned": {**NOAQE, "spark.sql.shuffle.partitions": "8"},
         "aqe": {"spark.sql.shuffle.partitions": "3000", "spark.sql.adaptive.enabled": "true",
                 "spark.sql.adaptive.coalescePartitions.enabled": "true"}},
        run_over_partitioned),
    "python_udf": Scenario(
        "python_udf", "Row-at-a-time Python UDF vs. pandas UDF vs. built-in expressions.",
        {"bad": {**NOAQE, "spark.sql.shuffle.partitions": "8"},
         "pandas_udf": {**NOAQE, "spark.sql.shuffle.partitions": "8"},
         "native": {**NOAQE, "spark.sql.shuffle.partitions": "8"}},
        run_python_udf),
    "join_strategy": Scenario(
        "join_strategy", "Dimension join forced to sort-merge vs. broadcast hash join.",
        {"bad": {**NOAQE, **NOBC, "spark.sql.shuffle.partitions": "8"},
         "broadcast": {**NOAQE, **NOBC, "spark.sql.shuffle.partitions": "8"}},
        run_join_strategy),
    "spill": Scenario(
        "spill", "RED HERRING: real spill in a 2-partition sort, but the sort is not the critical path.",
        {"bad": {**NOAQE, "spark.memory.fraction": "0.3", "spark.memory.storageFraction": "0.1"},
         "more_partitions": {**NOAQE, "spark.memory.fraction": "0.3", "spark.memory.storageFraction": "0.1"}},
        run_spill, driver_mem="768m"),
}


# ---------------------------------------------------------------- CLI
def cmd_run(a) -> None:
    sc = SCENARIOS[a.scenario]
    conf = dict(sc.variants[a.variant])
    logdir = os.path.join(a.out, "eventlog")
    shutil.rmtree(a.out, ignore_errors=True)
    spark = make_spark(f"{a.scenario}:{a.variant}", logdir, a.cores, conf, sc.driver_mem)
    # warm the JVM + parquet reader so first-run class loading is not billed to the workload
    spark.read.parquet(f"{a.data}/dim_g").count()
    t0_ms = int(time.time() * 1000)
    t0 = time.perf_counter()
    checksum = sc.run(spark, a.data, a.variant)
    wall = time.perf_counter() - t0
    t1_ms = int(time.time() * 1000)
    spark.stop()
    log = glob.glob(os.path.join(logdir, "*"))[0]
    json.dump({"scenario": a.scenario, "variant": a.variant, "wall_s": wall, "checksum": checksum,
               "window_ms": [t0_ms, t1_ms], "eventlog": log, "cores": a.cores},
              open(os.path.join(a.out, "meta.json"), "w"))
    print(f"{a.scenario}/{a.variant}: {wall:.2f}s checksum={checksum}")


def main(argv: Optional[List[str]] = None) -> None:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("setup")
    s.add_argument("--data", default="data")
    s.add_argument("--scale", type=float, default=1.0)
    r = sub.add_parser("run")
    r.add_argument("--scenario", required=True, choices=sorted(SCENARIOS))
    r.add_argument("--variant", required=True)
    r.add_argument("--data", default="data")
    r.add_argument("--out", required=True)
    r.add_argument("--cores", type=int, default=1)
    a = p.parse_args(argv)
    if a.cmd == "setup":
        setup_data(a.data, a.scale)
    else:
        cmd_run(a)


if __name__ == "__main__":
    main()
