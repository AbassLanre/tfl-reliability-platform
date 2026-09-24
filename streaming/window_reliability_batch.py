from pyspark.sql import SparkSession
from pyspark.sql.functions import (
   sum as sum_,
)
from streaming.silver_arrivals import  window_reliability  


if __name__ == "__main__":
  spark = (
      SparkSession.builder
      .appName("window_reliability_batch")
      .master("local[2]")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
  )


  silver_df = spark.read.parquet("data/silver/arrivals")


  window_reliability_df = window_reliability(silver_df)

  print(f"window_reliability_df count is {window_reliability_df.count()}")

  window_reliability_df.agg(
    sum_("n_completed").alias("total completed"),
    sum_("n_total").alias("total")
  ).show(truncate=False)
  window_reliability_df.write.mode("overwrite").parquet("data/silver/window_reliability")
  num = spark.read.parquet("data/silver/window_reliability").count()
  print(f"num is {num}")
