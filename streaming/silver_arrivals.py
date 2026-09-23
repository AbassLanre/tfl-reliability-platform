from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count_distinct, session_window, count, when, min as min_, min_by, max as max_, max_by,sum as sum_, avg, percentile, to_timestamp, window
)



def split_unattributable(df):
    unattributable = df.filter(col("vehicle_id") == '000')
    kept = df.filter((col("vehicle_id") != '000') & (col('event_type')=='arrival'))
    return kept, unattributable

def add_expected_ts(df):
    return df.withColumn("expected_arrival_ts", to_timestamp("expected_arrival"))

def dedupe_predictions(df):
    return df.dropDuplicates(["line_id", "vehicle_id", "naptan_id", "event_ts"])

def dedupe_predictions_within_stream(df):
    return (df
            .withWatermark("event_ts","2 minutes")
            .dropDuplicatesWithinWatermark(["line_id", "vehicle_id", "naptan_id", "event_ts"])
)

def build_arrivals(df, gap= "10 minutes"):
    return (
        df.groupBy(session_window("event_ts", gap), "line_id", "vehicle_id", "naptan_id")
    .agg( min_by('expected_arrival_ts', 'event_ts').alias("first_expected"),
         max_by('expected_arrival_ts', 'event_ts').alias("last_expected"),
      min_("time_to_station").alias("lowest_tts"),
      max_("event_ts").alias("last_event_seen"),
      count("*").alias("n_predictions"),
      ).withColumn("completed", col("lowest_tts") <=60)
    )
    
def add_delay(df):
    return df.withColumn("delay_s", (col("last_expected").cast('long') - col("first_expected").cast('long')))

def window_reliability(df, window_duration="5 minutes"):
    return (df
                 .groupBy(window("last_event_seen", window_duration),"line_id", "naptan_id")
                 .agg(
                     count("*").alias("n_total"),
                     count(when(col("completed"),1)).alias("n_completed"),
                     avg(when(col("completed"), col("delay_s"))).alias("avg_delay_s"),
                     min_(when(col("completed"), col("delay_s"))).alias("min_delay_s"),
                     max_(when(col("completed"), col("delay_s"))).alias("max_delay_s"),
                     percentile(when(col("completed"), col("delay_s")), 0.5).alias("p50_delay_s"),
                     percentile(when(col("completed"), col("delay_s")), 0.99).alias("p99_delay_s"),
                     count(when(~col("completed"),1)).alias("n_incomplete"),
                 )
                 )
if __name__ == "__main__":
    spark = (
    SparkSession.builder
    .appName("silver_arrivals")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
    )
    # Batch read once, only to learn the shape (22 columns and their types).
    bronze_schema = spark.read.parquet("data/bronze/arrivals").schema
    # Streaming read: same folder, but we hand it the shape up front,
    # because it will not peek at files itself (they may not exist yet).
    bronze = (spark.readStream
              .schema(bronze_schema)
              .option("maxFilesPerTrigger", 200)
              .parquet("data/bronze/arrivals")
)
    kept, unattributable = split_unattributable(bronze)
    kept = add_expected_ts(kept)
    preds = dedupe_predictions_within_stream(kept)
    grouped = build_arrivals(preds, gap="10 minutes")
    grouped = add_delay(grouped)
    
    query = (
        grouped.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", "data/silver/arrivals")
        .option("checkpointLocation", "data/checkpoints/silver_arrivals")
        .trigger(processingTime="10 seconds")
        .queryName("silver_arrivals")
        .start()
    )
    
    query.awaitTermination()
    # windowed_kept = window_reliability(grouped_kept)
    # windowed_kept.show(truncate=False)

    # windowed_kept.agg(
    #     sum_("n_incomplete").alias("total_incomplete_arrivals"),
    #     sum_("n_completed").alias("total_completed_arrivals"),
    #     sum_("n_total").alias("total_arrivals"),
    #     count("*").alias("n_windows"),
    # ).show(truncate=False)

