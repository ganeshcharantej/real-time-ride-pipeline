from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("RidePulse-Silver-Layer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema perfectly matches your raw Bronze data
bronze_schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("rider_id", StringType(), True),
    StructField("fare", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("pickup_location", StringType(), True)
])

bronze_stream = spark.readStream \
    .format("parquet") \
    .schema(bronze_schema) \
    .option("ignoreChanges", "true") \
    .load("data_lake/bronze_rides")

# Clean data: cast timestamp safely, and only filter by fare!
silver_stream = bronze_stream \
    .withColumn("ride_time", col("timestamp").cast("timestamp")) \
    .drop("timestamp") \
    .filter((col("ride_id").isNotNull()) & (col("fare") > 0))

query = silver_stream.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "data_lake/silver_rides") \
    .option("checkpointLocation", "data_lake/checkpoints_silver_final") \
    .start()

print("🚀 Silver transformation stream is active and cleaning data...")
query.awaitTermination()