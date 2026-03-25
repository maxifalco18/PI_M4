import sys
from pyspark.sql import SparkSession

if len(sys.argv) < 2:
    print("Usage: delta_maintenance_job.py <s3_gold_path_prefix>")
    sys.exit(1)

gold_prefix = sys.argv[1]

spark = SparkSession.builder \
    .appName("DeltaLakeMaintenance") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Listado de tablas Gold para optimizar
tables = [
    "gold_sales_by_category_time",
    "gold_sales_by_region",
    "gold_gold_sales_by_payment"
]

print(f"Iniciando mantenimiento Delta en: {gold_prefix}")

for table in tables:
    print(f"--- Optimizando tabla: {table} ---")
    
    # 1. OPTIMIZE: Compacta archivos pequeños y mejora el performance de Athena
    spark.sql(f"OPTIMIZE business_gold.{table}")
    
    # 2. VACUUM: Limpia archivos antiguos (retención por defecto 7 días)
    spark.sql(f"VACUUM business_gold.{table}")

print("Mantenimiento Delta completado exitosamente.")
