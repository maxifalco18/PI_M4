import sys
import os

try:
    from src.lib.environment import setup_environment
    logger = setup_environment()
except ImportError:
    # Fallback local/dev
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from src.lib.environment import setup_environment
    logger = setup_environment()

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from src.transformations.gold import (
    unify_lambda_orders, transform_gold_sales_by_category,
    transform_gold_sales_by_region, transform_gold_sales_by_payment
)

# 1. Inicialización y Parsing
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_SILVER', 'BUCKET_STREAMING', 'BUCKET_GOLD'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 2. Configuración de Catálogo 
spark.sql("CREATE DATABASE IF NOT EXISTS business_gold")
spark.sql("USE business_gold")

# 1. LEER DATOS BATCH (SILVER)
bucket_silver = args['BUCKET_SILVER']
bucket_processed_streaming = args['BUCKET_STREAMING'].replace("raw-streaming", "processed-streaming")
bucket_gold = args['BUCKET_GOLD']

df_orders_batch = spark.read.parquet(f"{bucket_silver}/fact_orders/")

# 2. LEER DATOS STREAMING (SPEED LAYER - PROCESSED)
df_orders_streaming = None
try:
    df_orders_streaming = spark.read.parquet(f"{bucket_processed_streaming}/olist_events/")
    print("Streaming data found.")
except:
    print("No streaming data found yet. Using batch data only.")

# 3. UNIFICACIÓN LAMBDA (Modular)
df_orders = unify_lambda_orders(df_orders_batch, df_orders_streaming)

# HITO 3: Optimización estratégica de Caché (Senior Pattern)
# Persistimos df_orders ya que se usa en 3 joins distintos para Capa Gold (Ventas, Región, Pago)
df_orders.persist()

# --------------------------------------------------------------------------
# MODELO DE NEGOCIO: VENTAS POR CATEGORIA Y TIEMPO (OBT)
# --------------------------------------------------------------------------
df_items = spark.read.parquet(f"{bucket_silver}/fact_order_items/")
df_products = spark.read.parquet(f"{bucket_silver}/dim_products/")

df_gold_sales = transform_gold_sales_by_category(df_items, df_products)

# Guardamos la OBT en formato DELTA usando MERGE
df_gold_sales.createOrReplaceTempView("updates")
spark.sql(f"""
    MERGE INTO business_gold.gold_sales_by_category_time t
    USING updates u
    ON t.product_category_name = u.product_category_name AND t.year = u.year AND t.month = u.month
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# --------------------------------------------------------------------------
# MODELO DE NEGOCIO: VENTAS POR REGION (CLIENTE)
# --------------------------------------------------------------------------
df_customers = spark.read.parquet(f"{bucket_silver}/dim_customers/")

df_gold_region = transform_gold_sales_by_region(df_orders, df_customers)

df_gold_region.createOrReplaceTempView("updates_region")
spark.sql("""
    MERGE INTO business_gold.gold_sales_by_region t
    USING updates_region u
    ON t.customer_state = u.customer_state AND t.year = u.year AND t.month = u.month
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# --------------------------------------------------------------------------
# MODELO DE NEGOCIO: VENTAS POR METODO DE PAGO
# --------------------------------------------------------------------------
df_payments = spark.read.parquet(f"{bucket_silver}/fact_payments/")

df_gold_payment = transform_gold_sales_by_payment(df_payments, df_orders)

df_gold_payment.createOrReplaceTempView("updates_payment")
spark.sql("""
    MERGE INTO business_gold.gold_gold_sales_by_payment t
    USING updates_payment u
    ON t.payment_type = u.payment_type AND t.year = u.year AND t.month = u.month
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

job.commit()
print("¡Capa GOLD terminada exitosamente!")