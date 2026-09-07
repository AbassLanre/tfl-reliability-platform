"""
Measure event-time skew (ingested_at - event_ts) on tfl.arrivals to choose
the Structured Streaming watermark delay. Batch read from Kafka, earliest.

Result 2026-09-07 over 1,547,523 arrivals (~3.7h, 26 Aug evening, no incidents):
  p50 4.5s | avg 15.6s | p99 64.3s | max 80.8s  -> watermark set to 2 minutes.
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, count, min as min_, max as max_, avg, percentile
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

spark = (
    SparkSession.builder
    .appName("measure_skew")
    .master("local[2]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

# TODO(you): read tfl.arrivals from Kafka as a batch DataFrame
df = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tfl.arrivals") \
    .load()

text = df.select(col("value").cast("string").alias("value_str"))

arrival_schema = StructType([
    StructField("event_type",  StringType()),
    StructField("event_ts",    TimestampType()),
    StructField("ingested_at", TimestampType()),
])

parsed = (
    text
    .select(from_json(col("value_str"), arrival_schema).alias("j"))
    .select("j.*")
)
parsed.groupBy("event_type").count().show()

skew = (
    parsed
    .filter(col("event_type") == "arrival")               # drops the 2 smoke_test rows
    .withColumn(
        "skew_s",
        col("ingested_at").cast("double") - col("event_ts").cast("double")
    )
)

skew.select("event_ts", "ingested_at", "skew_s").show(5, truncate=False)

skew.agg(
    count("*").alias("n"),
    min_("skew_s").alias("min_s"),
    avg("skew_s").alias("avg_s"),
    percentile("skew_s", 0.5).alias("p50_s"),
    percentile("skew_s", 0.99).alias("p99_s"),
    max_("skew_s").alias("max_s"),
).show(truncate=False)


spark.stop()