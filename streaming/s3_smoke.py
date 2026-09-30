from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, count, min as min_, max as max_, avg, percentile
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

spark = (
    SparkSession.builder
    .appName("s3_smoke")
    .master("local[2]")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.5.0") # adapter
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider") # "use aws keys"
    .config("spark.hadoop.fs.s3a.endpoint.region", "eu-west-2") # location
    .config("spark.sql.session.timeZone", "UTC") # standardized time
    .getOrCreate()
)

print(spark.version)
spark.range(5).write.mode("overwrite").parquet("s3a://tfl-reliability-raw-percy/_test/range")
print(f"count is {spark.read.parquet("s3a://tfl-reliability-raw-percy/_test/range").count()}")