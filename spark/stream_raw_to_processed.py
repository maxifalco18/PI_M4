# 1. Spark Session con soporte para Kafka
# Obtenemos parámetros vía línea de comandos (Spark-submit)
import sys
import os

# Ensure the root directory is in the path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from src.transformations.streaming import validate_event_schema

if len(sys.argv) < 4:
    print("Usage: stream_raw_to_processed.py <bootstrap_servers> <s3_output_path> <checkpoint_path>")
    sys.exit(1)

kafka_bootstrap_servers = sys.argv[1]
s3_output_path = sys.argv[2]
checkpoint_path = sys.argv[3]

spark = SparkSession.builder \
    .appName("OlistStreamingIngestion") \
    .getOrCreate()

# 2. Definición del Esquema (Pista: En Prod usar Glue Schema Registry)
# Aquí lo definimos manual, pero el código está preparado para integración vía boto3
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("order_value", DoubleType(), True)
])

# Integración Sugerida (Fase 4):
# from awsglue.utils import get_schema
# schema = get_schema("olist_events_registry")

# 3. Lectura desde Kafka (Configuración de Speed Layer)
topic = "olist_events"

df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load()

# 4. Transformación: Parsing, Watermarking y Validación Activa
df_raw_json = df_kafka.selectExpr("CAST(value AS STRING) as json_payload")

# ACTIVE ENFORCEMENT logic (Modular)
df_valid = df_raw_json.filter(validate_event_schema(col("json_payload")))

df_processed = df_valid.select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*") \
    .withWatermark("event_timestamp", "2 hours")

# 5. Escritura a S3 (Capa Raw-Streaming)
# Usamos un checkpointLocation para asegurar tolerancia a fallos

query = df_processed \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", s3_output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

query.awaitTermination()
