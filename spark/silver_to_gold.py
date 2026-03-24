import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import broadcast, sum as db_sum, count

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Solo leemos lo limpio de Silver!
bucket_silver = "s3://pi-m4-datalake-maxi/processed/batch"
bucket_gold = "s3://pi-m4-datalake-maxi/gold"

print("Iniciando procesamiento a capa GOLD...")

# --------------------------------------------------------------------------
# MODELO DE NEGOCIO: VENTAS POR CATEGORIA Y TIEMPO (OBT)
# --------------------------------------------------------------------------
# Leemos dos tablas Silver:
df_items = spark.read.parquet(f"{bucket_silver}/fact_order_items/")
df_products = spark.read.parquet(f"{bucket_silver}/dim_products/")

# El JOIN Maestro: usamos "broadcast" en products (tabla chica, ~33k filas).
# Esto copia 'products' a cada computadora del clúster de AWS y evita mover los 
# millones de 'items' por la red de AWS. ¡Es gratis, pero te hace ver súper Senior!
df_joined = df_items.join(broadcast(df_products), "product_id")

# Respondemos a la pregunta analítica:
df_gold_sales = df_joined.groupBy("product_category_name", "year", "month") \
                         .agg(
                             db_sum("price").alias("total_revenue"),
                             count("order_item_id").alias("total_items_sold")
                         )

# Guardamos la OBT particionada por año y mes para queries económicas en Athena
df_gold_sales.write.mode("overwrite").partitionBy("year", "month") \
             .parquet(f"{bucket_gold}/gold_sales_by_category_time/")

# --------------------------------------------------------------------------
# MODELO DE NEGOCIO: VENTAS POR REGION (CLIENTE)
# --------------------------------------------------------------------------
df_orders = spark.read.parquet(f"{bucket_silver}/fact_orders/")
df_customers = spark.read.parquet(f"{bucket_silver}/dim_customers/")

df_gold_region = df_orders.join(broadcast(df_customers), "customer_id") \
                          .groupBy("customer_state", "year", "month") \
                          .agg(count("order_id").alias("total_orders"))

df_gold_region.write.mode("overwrite").partitionBy("year", "month") \
              .parquet(f"{bucket_gold}/gold_sales_by_region/")

# --------------------------------------------------------------------------
# MODELO DE NEGOCIO: VENTAS POR METODO DE PAGO
# --------------------------------------------------------------------------
df_payments = spark.read.parquet(f"{bucket_silver}/fact_payments/")

# El join de orders con payments debe ser en order_id, aprovechando particionamiento temporal
df_gold_payment = df_payments.join(df_orders, "order_id") \
                             .groupBy("payment_type", "year", "month") \
                             .agg(db_sum("payment_value").alias("total_revenue"),
                                  count("order_id").alias("total_transactions"))

df_gold_payment.write.mode("overwrite").partitionBy("year", "month") \
               .parquet(f"{bucket_gold}/gold_sales_by_payment/")

job.commit()
print("¡Capa GOLD terminada exitosamente!")