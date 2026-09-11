from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("bronze_arrivals")
    .master("local[2]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)




good_count = spark.read.parquet("data/bronze/arrivals").count()
bad_df     = spark.read.text("data/quarantine/arrivals")

print(f"Good rows: {good_count}")
print(f"Bad rows: {bad_df.count()}")
print(bad_df.show(truncate=False))