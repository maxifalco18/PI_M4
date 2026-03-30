import sys
import os
import traceback
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, sum as db_sum, count, col
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable

from src.transformations.gold import (
    unify_lambda_orders,
    transform_sales_by_category,
    transform_sales_by_payment,
    transform_customer_segmentation,
    transform_order_value_metrics
)

try:
    # 1. SETUP & PARAMETERS
    # ──────────────────────────────────────────────────────────────────────────────
    # Recibimos parámetros desde Airflow
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 
        'BUCKET_SILVER', 
        'BUCKET_GOLD',
        'BUCKET_STREAMING'
    ])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    # Soporte para Delta Lake (Necesario si dim_customers es Delta)
    spark = glueContext.spark_session.builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    bucket_silver = args['BUCKET_SILVER']
    bucket_gold = args['BUCKET_GOLD']
    bucket_streaming = args['BUCKET_STREAMING']

    print("Iniciando procesamiento a capa GOLD...")

    # 2. READING SILVER LAYER
    # ──────────────────────────────────────────────────────────────────────────────
    # dim_customers es SCD Tipo 2 (Delta), leemos solo los vigentes
    df_customers = spark.read.format("delta").load(f"{bucket_silver}/dim_customers/") \
                        .filter("is_current = true")
    
    df_items = spark.read.parquet(f"{bucket_silver}/fact_order_items/")
    df_products = spark.read.parquet(f"{bucket_silver}/dim_products/")
    df_payments = spark.read.parquet(f"{bucket_silver}/fact_payments/")
    df_orders_batch = spark.read.parquet(f"{bucket_silver}/fact_orders/")

    # 3. READING STREAMING LAYER (Speed)
    # ──────────────────────────────────────────────────────────────────────────────
    try:
        df_streaming = spark.read.parquet(f"{bucket_streaming}/olist_events/")
    except Exception as e:
        print(f"No streaming data found or error reading: {e}")
        df_streaming = None

    # 4. UNIFY LAMBDA ARCHITECTURE
    # ──────────────────────────────────────────────────────────────────────────────
    df_orders = unify_lambda_orders(df_orders_batch, df_streaming)

    # 5. TRANSFORMATIONS (OBTs)
    # ──────────────────────────────────────────────────────────────────────────────
    
    # Pre-calculamos order_value desde fact_payments (Evita error de columna faltante en fact_orders)
    df_order_values = df_payments.groupBy("order_id").agg(db_sum("payment_value").alias("order_value"))

    # A. Sales by Category (Join Broadast con products)
    df_sales_cat = transform_sales_by_category(df_items, df_products, df_orders)
    df_sales_cat.write.mode("overwrite").partitionBy("year", "month") \
                .parquet(f"{bucket_gold}/gold_sales_by_category_time/")

    # B. Sales by Payment Type
    df_sales_pay = transform_sales_by_payment(df_payments, df_orders)
    df_sales_pay.write.mode("overwrite").partitionBy("payment_type") \
                .parquet(f"{bucket_gold}/gold_sales_by_payment/")

    # C. Customer Segmentation (RFM Basic)
    df_cust_seg = transform_customer_segmentation(df_orders, df_customers, df_order_values)
    df_cust_seg.write.mode("overwrite").parquet(f"{bucket_gold}/gold_customer_segmentation/")

    # D. Order Value Metrics
    df_order_metrics = transform_order_value_metrics(df_order_values, df_orders)
    df_order_metrics.write.mode("overwrite").parquet(f"{bucket_gold}/gold_order_value_metrics/")

    job.commit()
    print("¡Capa GOLD terminada exitosamente!")

except Exception as e:
    print("❌ FATAL ERROR IN GLUE GOLD JOB:")
    print(traceback.format_exc())
    raise e