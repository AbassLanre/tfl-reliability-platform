from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count_distinct, count, min as min_, max as max_, avg, percentile
)

spark = (
    SparkSession.builder
    .appName("silver_arrivals")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)


silver_df = spark.read.parquet("data/silver/arrivals")


print(f"silver rows: {silver_df.count()}")
print(f"completed rows: {silver_df.filter(col("completed")).count()}")

silver_df.agg(
    max_("last_event_seen").alias("max_seen"),
).show(truncate=False)

bronze_df = spark.read.parquet("data/bronze/arrivals")
bronze_df.filter((col("event_type") == 'arrival') & (col("vehicle_id") != "000")).agg(
    max_("event_ts").alias("max_event"),
).show(truncate=False)