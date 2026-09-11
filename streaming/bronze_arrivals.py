"""
Bronze: land every tfl.arrivals message as-is into Parquet, continuously
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, count, min as min_, max as max_, avg, percentile,
    to_date, hour, to_timestamp, date_format, current_timestamp
)
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

spark = (
    SparkSession.builder
    .appName("bronze_arrivals")
    .master("local[2]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

# TODO(you): read tfl.arrivals from Kafka as a batch DataFrame
df = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "tfl.arrivals")
    .option("startingOffsets", "earliest")                # streaming DEFAULT is "latest" = only new messages from now on; we want the backlog
    .option("maxOffsetsPerTrigger", "20000")              # cap per micro-batch: 20k rows per trigger, not 1.5M in one gulp
    .load()
)

text = df.select(col("value").cast("string").alias("value_str"))

arrival_schema = StructType([
    StructField("schema_version", IntegerType()),
    StructField("event_type",  StringType()),
    StructField("event_ts",    TimestampType()),
    StructField("ingested_at", TimestampType()),
    StructField('line_id', StringType()),
    StructField('time_to_station', IntegerType()),
    StructField('id', StringType()),
    StructField('vehicle_id', StringType()),
    StructField('naptan_id', StringType()),
    StructField('station_name', StringType()),
    StructField('line_name', StringType()), 
    StructField('platform_name', StringType()),
    StructField('current_location', StringType()),
    StructField('towards', StringType()),
    StructField('expected_arrival', StringType()),
    StructField('time_to_live', StringType()),
    StructField('mode_name', StringType()),
    StructField('direction', StringType()),
    StructField('destination_name', StringType()),
    StructField('destination_naptan_id', StringType()),
])

parsed = (
    text
    .select(col("value_str"),
            from_json(col("value_str"), arrival_schema).alias("j"))
)
is_bad = col("j.event_type").isNull() | col("j.event_ts").isNull()

good =( parsed
       .filter(~is_bad)
       .select("j.*")
       .withColumn("date", to_date(col("ingested_at")))
       .withColumn("hour", hour(col("ingested_at")))
       ) # ~ is NOT; expand the struct only for good rows
good.printSchema()

bad  = parsed.filter(is_bad).select("value_str")          # keep the raw text, that's what you'll debug from

good_q = (
    good.writeStream.queryName("bronze_good")
    .format("parquet")                                                   # WAS "console". Now: files on disk
    .option("path", "data/bronze/arrivals")                              # the shelves. Relative to where you run the script, so run from repo root
    .option("checkpointLocation", "data/checkpoints/bronze_arrivals")    # the clipboard: which Kafka offsets are finished. One per query, never shared
    .partitionBy("date", "hour")                                         # one folder per value: date=2026-09-08/hour=19/
    .outputMode("append")                                                # unchanged (also the only mode the file sink supports)
    .trigger(processingTime="10 seconds")                                # unchanged
    .start()
)
bad_q  = (
        bad.writeStream.queryName("bronze_quarantine")
        .format("text")
        .option("path", "data/quarantine/arrivals")                              # the shelves. Relative to where you run the script, so run from repo root
        .option("checkpointLocation", "data/checkpoints/bronze_quarantine")    # the clipboard: which Kafka offsets are finished. One per query, never shared
        .outputMode("append")                                                # unchanged (also the only mode the file sink supports)
        .trigger(processingTime="10 seconds")                                # unchanged
        .start()
          )
spark.streams.awaitAnyTermination()                                 # block the main thread here until Ctrl+C or a failure