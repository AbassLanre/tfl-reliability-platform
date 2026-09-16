from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count_distinct, count, min as min_, max as max_, avg, percentile
)

spark = (
    SparkSession.builder
    .appName("bronze_arrivals")
    .master("local[2]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)




# good_count = spark.read.parquet("data/bronze/arrivals").count()
bad_df     = spark.read.text("data/quarantine/arrivals")
good_df = spark.read.parquet("data/bronze/arrivals")


# print(f"Good rows: {good_df.count()}")
# print(f"Bad rows: {bad_df.count()}")
# print(bad_df.show(truncate=False))
# counts=good_df.groupBy('id').count()

# counts.show()

# counts.agg(
#     count("*").alias("n"),
#     min_("count").alias("min_count"),
#     avg("count").alias("avg_count"),
#     percentile("count", 0.5).alias("p50_count"),
#     percentile("count", 0.99).alias("p99_count"),
#     max_("count").alias("max_count"),
# ).show(truncate=False)

# selected_id = counts.orderBy(col("count").desc()).select("id").first()[0]
# counts.orderBy(col("count").desc()).show()
# print(f"Selected id: {selected_id}")

# good_df.filter(col("id") == selected_id).agg(
#     count_distinct("naptan_id").alias("Naptan IDs"),
#     count_distinct("event_ts").alias("Event Timestamps"),
#     count_distinct("vehicle_id").alias("Vehicle IDs")
#     ).show()

# percent_000 = (good_df.filter(col("vehicle_id") == '000').count()/good_df.count()) * 100

# good_df.select("direction","platform_name","current_location","ingested_at", "event_ts","time_to_station","line_id", "vehicle_id", "station_name", "destination_name").filter(col("id") == selected_id).orderBy(col("ingested_at")).show(20, truncate=False)

# hypothesis_test = good_df.filter(col("vehicle_id") != '000').groupBy('line_id', 'vehicle_id','naptan_id','event_ts',"platform_name", "current_location").count()

# victoria_line = good_df.filter((col("line_id") == 'victoria') & (col("naptan_id") == '940GZZLUSVS') & (col("platform_name") =="Northbound - Platform 3" ) & (col("current_location") == "Approaching Pimlico") & (col("event_ts") == "2026-09-09 18:48:17.296044") ).select('time_to_station', 'expected_arrival',"ingested_at",'event_ts',"platform_name")
# victoria_line.show(truncate=False)

# hypothesis_test.agg(
#     count("*").alias("n"),
#     min_("count").alias("min_count"),
#     avg("count").alias("avg_count"),
#     percentile("count", 0.5).alias("p50_count"),
#     percentile("count", 0.99).alias("p99_count"),
#     max_("count").alias("max_count"),
# ).show(truncate=False)

lowest = (
    good_df.filter(col("vehicle_id") != "000")
    .groupBy("line_id", "vehicle_id", "naptan_id")
    .agg(min_("time_to_station").alias("lowest_tts"))
)

lowest.agg(
    count("*").alias("n_arrivals"),
    min_("lowest_tts").alias("min"),
    avg("lowest_tts").alias("avg"),
    percentile("lowest_tts", 0.5).alias("p50"),
    percentile("lowest_tts", 0.99).alias("p99"),
    max_("lowest_tts").alias("max"),
).show(truncate=False)

percent_pred= lowest.filter(col("lowest_tts") <= 60).count()/lowest.count() * 100
print(percent_pred)