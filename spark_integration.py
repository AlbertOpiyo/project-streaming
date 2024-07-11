import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_HOME'] = "C:\spark-3.5.1-bin-hadoop3"
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace, substring
from pyspark.sql.types import StructType, StructField, StringType, FloatType,IntegerType,BooleanType,TimestampType,\
    ArrayType, MapType
from pyspark.sql.functions import expr

# localhost:27017
# Initialize Spark session
spark = SparkSession.builder \
    .appName("CoinMarketKafkaSpark") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/coinmarket.data") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/coinmarket.data") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()


# Define data schema
data_schema = StructType([
    StructField("circulating_supply", IntegerType(), True),
    StructField("cmc_rank", IntegerType(), True),
    StructField("date_added", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("infinite_supply", BooleanType(), True),
    StructField("last_updated", StringType(), True),
    StructField("max_supply", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("num_market_pairs", IntegerType(), True),
    StructField("platform", StringType(), True),
    # Define nested quote schema
    StructField("quote", MapType(StringType(), StructType([
        StructField("fully_diluted_market_cap", FloatType(), True),
        StructField("last_updated", StringType(), True),
        StructField("market_cap", FloatType(), True),
        StructField("market_cap_dominance", FloatType(), True),
        StructField("percent_change_1h", FloatType(), True),
        StructField("percent_change_24h", FloatType(), True),
        StructField("percent_change_30d", FloatType(), True),
        StructField("percent_change_60d", FloatType(), True),
        StructField("percent_change_7d", FloatType(), True),
        StructField("percent_change_90d", FloatType(), True),
        StructField("price", FloatType(), True),
        StructField("tvl", StringType(), True),
        StructField("volume_24h", FloatType(), True),
        StructField("volume_change_24h", FloatType(), True),
    ]))),
    StructField("self_reported_circulating_supply", StringType(), True),
    StructField("self_reported_market_cap", StringType(), True),
    StructField("slug", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("total_supply", IntegerType(), True),
    StructField("tvl_ratio", StringType(), True),
])

# Define status schema
status_schema = StructType([
    StructField("credit_count", IntegerType(), True),
    StructField("elapsed", IntegerType(), True),
    StructField("error_code", IntegerType(), True),
    StructField("error_message", StringType(), True),
    StructField("notice", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("total_count", IntegerType(), True),
])

# Define top level schema
schema = StructType([
    StructField("data", ArrayType(data_schema)),
    StructField("status", status_schema),
])


# print(schema)

# Read data from Kafka
kafka_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "coinmarket") \
    .load()

kafka_stream_df.printSchema()

#  Process data
kafka_stream_df = kafka_stream_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


kafka_stream_df.printSchema()

# # Write data to MongoDB
# kafka_stream_df.writeStream \
#     .format("mongo") \
#     .outputMode("append") \
#     .option("database", "coinmarket") \
#     .option("collection", "data") \
#     .start() \
#     .awaitTermination()

# Clean the directories if they exist
import shutil
import os
parquet_path = "C:/Users/ADMIN/AppData/Local/Temp/coinmarket_parquet"
checkpoint_path = "C:/Users/ADMIN/AppData/Local/Temp/coinmarket_checkpoint"

if os.path.exists(parquet_path):
    shutil.rmtree(parquet_path)
if os.path.exists(checkpoint_path):
    shutil.rmtree(checkpoint_path)


# Write the streaming data to a temporary Parquet file
kafka_stream_df.writeStream \
    .format("parquet") \
    .option("path", parquet_path) \
    .option("checkpointLocation", checkpoint_path) \
    .start() \
    .awaitTermination()

# Now, you can set up a batch job to read from the Parquet files and write to MongoDB
batch_df = spark.read.parquet(parquet_path)
batch_df.write \
    .format("mongodb") \
    .option("spark.mongodb.connection.uri", "mongodb://127.0.0.1:27017/coinmarket.data") \
    .mode("append") \
    .save()