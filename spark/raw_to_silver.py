import sys
import os
import logging
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col, lit, current_timestamp, current_date
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable

from src.transformations.silver import (
    transform_geolocation, transform_orders, transform_payments,
    transform_customers, transform_sellers, transform_products,
    transform_order_items, transform_order_reviews
)

# ──────────────────────────────────────────────────────────────────────────────
# 1. Inicialización 100% Nube y Parsing de Parámetros
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_IN', 'BUCKET_OUT'])

sc = SparkContext()
glueContext = GlueContext(sc)
# Habilitamos soporte nativo para Delta Lake (Indispensable para SCD Tipo 2)
spark = glueContext.spark_session.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 2. Configuración de Catálogo (Habilita consultas en Athena)
spark.sql("CREATE DATABASE IF NOT EXISTS processed_silver")
spark.sql("USE processed_silver")

# 2. Configuración de Performance
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic") # Clave para incrementalidad básica

# Rutas parametrizadas
bucket_in = args['BUCKET_IN']
bucket_out = args['BUCKET_OUT']

print("Iniciando procesamiento a capa SILVER...")

# --------------------------------------------------------------------------
# DIM_GEOLOCATION
# --------------------------------------------------------------------------
df_geo = spark.read.parquet(f"{bucket_in}/olist_geolocation/")
df_geo = transform_geolocation(df_geo)
df_geo.write.mode("overwrite").partitionBy("geolocation_state") \
    .option("path", f"{bucket_out}/dim_geolocation/") \
    .saveAsTable("dim_geolocation")

# --------------------------------------------------------------------------
# FACT_ORDERS
# --------------------------------------------------------------------------
df_orders = spark.read.parquet(f"{bucket_in}/olist_orders/")
df_orders = transform_orders(df_orders)
df_orders.write.mode("overwrite").partitionBy("year", "month") \
    .option("path", f"{bucket_out}/fact_orders/") \
    .saveAsTable("fact_orders")

# --------------------------------------------------------------------------
# FACT_PAYMENTS
# --------------------------------------------------------------------------
df_payments = spark.read.parquet(f"{bucket_in}/olist_order_payments/")
df_payments = transform_payments(df_payments)
df_payments.write.mode("overwrite").partitionBy("payment_type") \
    .option("path", f"{bucket_out}/fact_payments/") \
    .saveAsTable("fact_payments")

# --------------------------------------------------------------------------
# DIM_CUSTOMERS (SCD Type 2)
# --------------------------------------------------------------------------
df_customers_new = spark.read.parquet(f"{bucket_in}/olist_customers/")
df_customers_new = transform_customers(df_customers_new)

target_path_cust = f"{bucket_out}/dim_customers/"
# En un entorno de producción, revisaríamos si la tabla existe en el catálogo
table_exists = spark.catalog.tableExists("processed_silver.dim_customers")

if not table_exists:
    df_customers_new.write.format("delta").mode("overwrite").partitionBy("customer_state") \
        .option("path", target_path_cust) \
        .saveAsTable("dim_customers")
else:
    dt = DeltaTable.forPath(spark, target_path_cust)
    dt.alias("t").merge(
        df_customers_new.alias("u"),
        "t.customer_id = u.customer_id"
    ).whenMatchedUpdate(set = {
        "end_date": "current_date()",
        "is_current": "false"
    }).whenNotMatchedInsertAll().execute()

# --------------------------------------------------------------------------
# DIM_SELLERS
# --------------------------------------------------------------------------
df_sellers = spark.read.parquet(f"{bucket_in}/olist_sellers/")
df_sellers = transform_sellers(df_sellers)
df_sellers.write.mode("overwrite").partitionBy("seller_state") \
    .option("path", f"{bucket_out}/dim_sellers/") \
    .saveAsTable("dim_sellers")

# --------------------------------------------------------------------------
# DIM_PRODUCTS
# --------------------------------------------------------------------------
df_products = spark.read.parquet(f"{bucket_in}/olist_products/")
df_products = transform_products(df_products)
df_products.write.mode("overwrite") \
    .option("path", f"{bucket_out}/dim_products/") \
    .saveAsTable("dim_products")

# --------------------------------------------------------------------------
# FACT_ORDER_ITEMS
# --------------------------------------------------------------------------
df_order_items = spark.read.parquet(f"{bucket_in}/olist_order_items/")
df_order_items = transform_order_items(df_order_items)
df_order_items.write.mode("overwrite") \
    .option("path", f"{bucket_out}/fact_order_items/") \
    .saveAsTable("fact_order_items")

# --------------------------------------------------------------------------
# FACT_ORDER_REVIEWS
# --------------------------------------------------------------------------
df_reviews = spark.read.parquet(f"{bucket_in}/olist_order_reviews/")
df_reviews = transform_order_reviews(df_reviews)
df_reviews.write.mode("overwrite") \
    .option("path", f"{bucket_out}/fact_order_reviews/") \
    .saveAsTable("fact_order_reviews")

job.commit()
print("¡Capa SILVER terminada exitosamente!")