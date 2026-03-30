import sys
import os

# Ensure the root directory is in the path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, BooleanType
from src.transformations.streaming import validate_event_schema

if len(sys.argv) < 4:
    print("Usage: kafka_to_raw_streaming.py <bootstrap_servers> <s3_output_path> <checkpoint_path>")
    sys.exit(1)

kafka_bootstrap_servers = sys.argv[1]
s3_output_path = sys.argv[2]
checkpoint_path = sys.argv[3]

spark = SparkSession.builder \
    .appName("OlistKafkaIngestionRaw") \
    .getOrCreate()

# 1. Definición del Esquema Inicial
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("order_value", DoubleType(), True)
])

# Register UDF for structural validation
validate_schema_udf = udf(lambda x: validate_event_schema(x), BooleanType())

topic = "olist_events"

# 2. Lectura Nativa desde Kafka
df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# 3. Transformación Básica: Parsing, Watermarking y Validación
df_raw_json = df_kafka.selectExpr("CAST(value AS STRING) as json_payload")

# Aplicar gate lógico usando el validator de la librería `src`
df_valid = df_raw_json.filter(validate_schema_udf(col("json_payload")))

# Parsear strings asumiendo un formato válido
df_parsed = df_valid.select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*") \
    .withWatermark("event_timestamp", "2 hours")

# 4. Escritura Directa a la Capa "raw-streaming" en S3
# Se cumple el requisito "consuma datos desde Kafka... y los escriba en la capa raw-streaming en formato Parquet"
query = df_parsed \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", s3_output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

query.awaitTermination()
