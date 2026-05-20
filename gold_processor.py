from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("RidePulse-Gold-Layer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Matches the new Silver output
silver_schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("rider_id", StringType(), True),
    StructField("fare", DoubleType(), True),
    StructField("pickup_location", StringType(), True),
    StructField("ride_time", TimestampType(), True)
])

silver_stream = spark.readStream \
    .format("parquet") \
    .schema(silver_schema) \
    .load("data_lake/silver_rides")

# Dashboard metrics
gold_metrics = silver_stream.groupBy() \
    .agg(
        sum("fare").alias("total_revenue"),
        count("ride_id").alias("total_rides"),
        avg("fare").alias("average_fare")
    )

query = gold_metrics.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("checkpointLocation", "data_lake/checkpoints_gold_new") \
    .start()

print("📈 Gold aggregation dashboard engine is active. Waiting for data... \n")
query.awaitTermination()