import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
# awsglue es la librería oficial de AWS que viene instalada en tu clúster invisible.
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import year, month

# 1. Inicialización 100% Nube
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# 2. Configuración de Performance (Soluciona el problema de "Small Files" de Athena)
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

# Rutas
bucket_in = "s3://pi-m4-datalake-maxi/raw/batch"
bucket_out = "s3://pi-m4-datalake-maxi/processed/batch"

print("Iniciando procesamiento a capa SILVER...")

# --------------------------------------------------------------------------
# DIM_GEOLOCATION (Deduplicación agresiva de millones a miles)
# --------------------------------------------------------------------------
df_geo = spark.read.parquet(f"{bucket_in}/olist_geolocation/")
df_geo = df_geo.dropDuplicates(['geolocation_zip_code_prefix'])
df_geo.write.mode("overwrite").partitionBy("geolocation_state").parquet(f"{bucket_out}/dim_geolocation/")

# --------------------------------------------------------------------------
# FACT_ORDERS (Particionamiento Temporal)
# --------------------------------------------------------------------------
df_orders = spark.read.parquet(f"{bucket_in}/olist_orders/")
df_orders = df_orders.withColumn("year", year("order_purchase_timestamp")) \
                     .withColumn("month", month("order_purchase_timestamp"))
df_orders.write.mode("overwrite").partitionBy("year", "month").parquet(f"{bucket_out}/fact_orders/")

# --------------------------------------------------------------------------
# FACT_PAYMENTS (Particionamiento Categórico)
# --------------------------------------------------------------------------
df_payments = spark.read.parquet(f"{bucket_in}/olist_order_payments/")
df_payments.write.mode("overwrite").partitionBy("payment_type").parquet(f"{bucket_out}/fact_payments/")

# --------------------------------------------------------------------------
# DIM_CUSTOMERS
# --------------------------------------------------------------------------
df_customers = spark.read.parquet(f"{bucket_in}/olist_customers/")
df_customers = df_customers.dropDuplicates(['customer_id'])
df_customers.write.mode("overwrite").partitionBy("customer_state").parquet(f"{bucket_out}/dim_customers/")

# --------------------------------------------------------------------------
# DIM_SELLERS
# --------------------------------------------------------------------------
df_sellers = spark.read.parquet(f"{bucket_in}/olist_sellers/")
df_sellers = df_sellers.dropDuplicates(['seller_id'])
df_sellers.write.mode("overwrite").partitionBy("seller_state").parquet(f"{bucket_out}/dim_sellers/")

# --------------------------------------------------------------------------
# DIM_PRODUCTS
# --------------------------------------------------------------------------
df_products = spark.read.parquet(f"{bucket_in}/olist_products/")
df_products = df_products.dropDuplicates(['product_id'])
df_products.write.mode("overwrite").parquet(f"{bucket_out}/dim_products/")

# --------------------------------------------------------------------------
# FACT_ORDER_ITEMS
# --------------------------------------------------------------------------
df_order_items = spark.read.parquet(f"{bucket_in}/olist_order_items/")
df_order_items = df_order_items.dropDuplicates(['order_id', 'order_item_id'])
df_order_items.write.mode("overwrite").parquet(f"{bucket_out}/fact_order_items/")

# --------------------------------------------------------------------------
# FACT_ORDER_REVIEWS
# --------------------------------------------------------------------------
df_reviews = spark.read.parquet(f"{bucket_in}/olist_order_reviews/")
df_reviews = df_reviews.dropDuplicates(['review_id'])
df_reviews.write.mode("overwrite").parquet(f"{bucket_out}/fact_order_reviews/")

job.commit()
print("¡Capa SILVER terminada exitosamente!")