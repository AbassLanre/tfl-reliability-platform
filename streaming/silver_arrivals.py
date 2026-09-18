from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count_distinct, session_window, count, min as min_, min_by, max as max_, max_by, avg, percentile, to_timestamp
)

spark = (
    SparkSession.builder
    .appName("silver_arrivals")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

bronze = spark.read.parquet("data/bronze/arrivals")

unattributable = bronze.filter(col("vehicle_id") == '000')
kept = bronze.filter((col("vehicle_id") != '000') & (col('event_type')=='arrival'))

kept = kept.withColumn("expected_arrival_ts", to_timestamp("expected_arrival"))
without_duplicates = kept.dropDuplicates(["line_id", "vehicle_id", "naptan_id", "event_ts"])
# measure_expected_bool = kept.withColumn("expected_arrival_bool", (col("time_to_station") == ( col("expected_arrival_ts").cast('long') - col("event_ts").cast('long'))))

# measure_expected_bool_true = measure_expected_bool.filter(col("expected_arrival_bool") == True)
# print(kept.count())

grouped_kept = (
    without_duplicates.groupBy(session_window("event_ts", "10 minutes"), "line_id", "vehicle_id", "naptan_id")
    .agg( min_by('expected_arrival_ts', 'event_ts').alias("first_expected"),
         max_by('expected_arrival_ts', 'event_ts').alias("last_expected"),
      min_("time_to_station").alias("lowest_tts"),
      max_("event_ts").alias("last_event_seen"),
      count("*").alias("n_predictions"),
      ).withColumn("completed", col("lowest_tts") <=60)
    .withColumn("delay_s", (col("last_expected").cast('long') - col("first_expected").cast('long')))
).cache()

# grouped_kept.agg(
#     count("*").alias("n"),
#     min_("n_predictions").alias("min_predictions"),
#     avg("n_predictions").alias("avg_predictions"),
#     percentile("n_predictions", 0.5).alias("p50_predictions"),
#     percentile("n_predictions", 0.99).alias("p99_predictions"),
#     max_("n_predictions").alias("max_predictions"),  
#     max_("delay_s").alias("max_delay_s"),  
# ).show(truncate=False) 

grouped_kept.filter(col("completed")).agg(
   count("*").alias("n"),
    min_("delay_s").alias("min_delay_s"),
    avg("delay_s").alias("avg_delay_s"),
    percentile("delay_s", 0.5).alias("p50_delay_s"),
    percentile("delay_s", 0.99).alias("p99_delay_s"),
    max_("delay_s").alias("max_delay_s"),  
).show(truncate=False)


# completed_pct = grouped_kept.filter(col("completed")).count() / grouped_kept.count() * 100
# print(f"Percentage of completed arrivals: {completed_pct:.2f}%")
delay_cnt = grouped_kept.filter(col("completed")).filter(col("delay_s") > 3600).count()
print(f"Number of completed arrivals with delay: {delay_cnt}")
