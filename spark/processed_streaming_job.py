import sys
import os

# Ensure the root directory is in the path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from awsglue.utils import getResolvedOptions
from src.transformations.streaming import transform_speed_layer

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    's3_raw_path',
    's3_processed_path',
    'checkpoint_path'
])

raw_path = args['s3_raw_path']
processed_path = args['s3_processed_path']
checkpoint_path = args['checkpoint_path']

spark = SparkSession.builder \
    .appName("OlistSpeedLayerTransformation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
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
