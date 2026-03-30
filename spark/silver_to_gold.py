import sys
import os
import logging
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as db_sum, count, lit, current_timestamp, broadcast
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ──────────────────────────────────────────────────────────────────────────────
# 1. SETUP & PARAMETERS
# ──────────────────────────────────────────────────────────────────────────────
# Recibimos parámetros desde Airflow
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'BUCKET_SILVER', 
    'BUCKET_GOLD'
])

sc = SparkContext()
glueContext = GlueContext(sc)

# Habilitamos soporte Delta para leer dim_customers correctamente
spark = glueContext.spark_session.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SILVER = args['BUCKET_SILVER']
GOLD   = args['BUCKET_GOLD']

print(f"Iniciando Silver -> Gold. Leyendo de {SILVER}...")

# ──────────────────────────────────────────────────────────────────────────────
# 2. LECTURA DE CAPA SILVER
# ──────────────────────────────────────────────────────────────────────────────
df_orders = spark.read.parquet(f"{SILVER}/fact_orders/")
df_items = spark.read.parquet(f"{SILVER}/fact_order_items/")
df_products = spark.read.parquet(f"{SILVER}/dim_products/")
df_payments = spark.read.parquet(f"{SILVER}/fact_payments/")

# Lectura de DIM_CUSTOMERS (Delta Table + SCD Tipo 2 Filter)
df_customers = spark.read.format("delta").load(f"{SILVER}/dim_customers/") \
                    .filter(col("is_current") == True)

# Pre-agregación de pagos para obtener el valor total por orden
df_order_values = df_payments.groupBy("order_id").agg(db_sum("payment_value").alias("order_value"))

# ──────────────────────────────────────────────────────────────────────────────
# 3. TRANSFORM & WRITE (GOLD LAYER)
# ──────────────────────────────────────────────────────────────────────────────

# GOLD: VENTAS POR CATEGORÍA
# Usamos 'price' de fact_order_items que es el campo correcto para revenue por ítem
df_gold_sales = df_items.join(broadcast(df_products), "product_id") \
                        .groupBy("product_category_name", "year", "month") \
                        .agg(db_sum("price").alias("total_revenue"), 
                             count("order_item_id").alias("total_items_sold"))

df_gold_sales.write.mode("overwrite").parquet(f"{GOLD}/gold_sales_by_category_time/")
print("gold_sales_by_category_time OK")

# GOLD: VENTAS POR REGION
# Unimos orders con el valor total calculado y customers
df_gold_region = df_orders.join(df_order_values, "order_id") \
                          .join(broadcast(df_customers), "customer_id") \
                          .groupBy("customer_state", "year", "month") \
                          .agg(db_sum("order_value").alias("total_sales"))

df_gold_region.write.mode("overwrite").parquet(f"{GOLD}/gold_sales_by_region/")
print("gold_sales_by_region OK")

# GOLD: VENTAS POR METODO DE PAGO
# Usamos 'payment_value' directamente de la tabla de pagos
df_gold_payment = df_payments.join(broadcast(df_orders), "order_id") \
                           .groupBy("payment_type", "year", "month") \
                           .agg(db_sum("payment_value").alias("total_sales"))

df_gold_payment.write.mode("overwrite").parquet(f"{GOLD}/gold_sales_by_payment/")
print("gold_sales_by_payment OK")

job.commit()
print("Finalizado!")