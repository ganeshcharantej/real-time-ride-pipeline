from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Initialize Spark Session with Kafka Support
spark = SparkSession.builder \
    .appName("RidePulseLocalDataLake") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define the Schema
ride_schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("rider_id", StringType(), True),
    StructField("pickup_location", StringType(), True),
    StructField("fare", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# 3. Read Stream from Kafka
raw_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092")\
    .option("subscribe", "ride_events") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Transform and Clean Data
parsed_stream_df = raw_stream_df \
    .selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), ride_schema).alias("data")) \
    .select("data.*") \
    .filter(col("fare").isNotNull())

# 5. Output: Write the streaming data to a Local Data Lake directory
# We partition by 'pickup_location' just like a real enterprise data lake
query = parsed_stream_df.writeStream \
    .format("parquet") \
    .option("path", "./data_lake/bronze_rides") \
    .option("checkpointLocation", "./data_lake/checkpoints") \
    .partitionBy("pickup_location") \
    .start()

print("PySpark is now writing the live stream into your local Data Lake (./data_lake/bronze_rides)...")
query.awaitTermination()