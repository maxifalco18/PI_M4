import sys
import os
import logging
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as db_sum, count, lit, current_timestamp, broadcast
from awsglue.context import GlueContext
from awsglue.job import Job

# ──────────────────────────────────────────────────────────────────────────────
# 1. SETUP
# ──────────────────────────────────────────────────────────────────────────────
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("job_silver_to_gold", {})

BUCKET = "s3://pi-m4-datalake-maxi"
SILVER = f"{BUCKET}/processed/batch"
GOLD   = f"{BUCKET}/gold"

print(f"Iniciando Silver -> Gold. Leyendo de {SILVER}...")

# ──────────────────────────────────────────────────────────────────────────────
# 2. TRANSFORM & WRITE (Parquet over S3, no Delta merge to avoid Jars issues)
# ──────────────────────────────────────────────────────────────────────────────
# LECTURA
df_orders = spark.read.parquet(f"{SILVER}/fact_orders/")
df_items = spark.read.parquet(f"{SILVER}/fact_order_items/")
df_products = spark.read.parquet(f"{SILVER}/dim_products/")
df_customers = spark.read.parquet(f"{SILVER}/dim_customers/")
df_payments = spark.read.parquet(f"{SILVER}/fact_payments/")

# GOLD: VENTAS POR CATEGORÍA
df_gold_sales = df_items.join(broadcast(df_products), "product_id") \
                        .groupBy("product_category_name", "year", "month") \
                        .agg(db_sum("price").alias("total_revenue"), count("order_item_id").alias("total_items_sold"))

df_gold_sales.write.mode("overwrite").parquet(f"{GOLD}/gold_sales_by_category_time/")
print("gold_sales_by_category_time OK")

# GOLD: VENTAS POR REGION
df_gold_region = df_orders.join(broadcast(df_customers), "customer_id") \
                          .groupBy("customer_state", "year", "month") \
                          .agg(db_sum("order_value").alias("total_sales"))

df_gold_region.write.mode("overwrite").parquet(f"{GOLD}/gold_sales_by_region/")
print("gold_sales_by_region OK")

# GOLD: VENTAS POR METODO DE PAGO
df_gold_payment = df_payments.join(broadcast(df_orders), "order_id") \
                           .groupBy("payment_type", "year", "month") \
                           .agg(db_sum("order_value").alias("total_sales"))

df_gold_payment.write.mode("overwrite").parquet(f"{GOLD}/gold_sales_by_payment/")
print("gold_sales_by_payment OK")

job.commit()
print("Finalizado!")