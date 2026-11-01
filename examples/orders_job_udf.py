from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

spark = (SparkSession.builder.master("local[4]").appName("orders-report")
         .config("spark.driver.memory", "2g")
         .config("spark.eventLog.enabled", "true")
         .config("spark.eventLog.dir", "file:///home/raghavendra/eventlogs")
         .config("spark.ui.enabled", "false")
         .getOrCreate())

orders = (spark.range(3_000_000)
          .withColumn("customer_id", (F.rand(1) * 100000).cast("int"))
          .withColumn("roll", F.rand(2))
          .withColumn("city", F.when(F.col("roll") < 0.6, "Bengaluru")
                      .when(F.col("roll") < 0.8, "Mysuru")
                      .when(F.col("roll") < 0.9, "Hubballi")
                      .otherwise("Mangaluru"))
          .withColumn("amount", (F.rand(3) * 500).cast("int")))

customers = (spark.range(100000).withColumnRenamed("id", "customer_id")
             .withColumn("segment", (F.col("customer_id") % 5).cast("string")))

def tier(a):
    return "high" if a > 400 else ("mid" if a > 150 else "low")
tier_udf = F.udf(tier, StringType())

report = (orders.join(customers, "customer_id")
          .withColumn("tier", tier_udf("amount"))
          .groupBy("city", "segment", "tier")
          .agg(F.count("*").alias("orders"), F.sum("amount").alias("revenue"))
          .orderBy("city", "segment", "tier"))

report.write.mode("overwrite").parquet("/home/raghavendra/orders_report")
spark.stop()
