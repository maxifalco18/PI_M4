import sys
import os

# Ensure the root directory is in the path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from src.transformations.streaming import transform_speed_layer

if len(sys.argv) < 3:
    print("Usage: processed_streaming_job.py <s3_raw_path> <s3_processed_path> <checkpoint_path>")
    sys.exit(1)

raw_path = sys.argv[1]
processed_path = sys.argv[2]
checkpoint_path = sys.argv[3]

spark = SparkSession.builder \
    .appName("OlistSpeedLayerTransformation") \
    .getOrCreate()

# 1. Lectura de la capa RAW Streaming (Parquet)
df_raw = spark.readStream \
    .format("parquet") \
    .schema("order_id STRING, customer_id STRING, order_status STRING, event_timestamp TIMESTAMP, order_value DOUBLE") \
    .load(raw_path) \
    .withWatermark("event_timestamp", "2 hours") 

# 2. Transformaciones de la Capa Processed (Modular)
df_processed = transform_speed_layer(df_raw) \
    .withColumn("processing_timestamp", expr("current_timestamp()"))

# 3. Escritura a la capa PROCESSED Streaming
query = df_processed \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", processed_path) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

query.awaitTermination()
