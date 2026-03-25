import sys
from pyspark.sql import SparkSession

if len(sys.argv) < 2:
    print("Usage: silver_compaction_job.py <s3_silver_base_path>")
    sys.exit(1)

silver_base = sys.argv[1].rstrip("/")
spark = SparkSession.builder \
    .appName("SilverLayerCompaction") \
    .config("spark.sql.parquet.fs.optimized.committer.optimization-enabled", "true") \
    .getOrCreate()

# Listado de tablas en Silver (Parquet)
tables = ["dim_geolocation", "fact_orders", "fact_payments", "dim_customers", "dim_sellers", "dim_products", "fact_order_items", "fact_order_reviews"]

for table in tables:
    print(f"--- Compactando tabla Silver: {table} ---")
    s3_path = f"{silver_base}/{table}/"
    
    # Leemos la tabla (con cientos de archivos pequeños)
    df = spark.read.parquet(s3_path)
    
    # REPARTITION/COALESCE: Reducimos a un número óptimo de archivos grandes (e.g., 128MB-256MB)
    # Aquí aproximamos un número bajo de archivos para Athena.
    df_compacted = df.coalesce(2) 
    
    # Escribimos de vuelta con OVERWRITE (Nota: El modo dinámico en Silver lo hace seguro)
    df_compacted.write.mode("overwrite") \
                .option("partitionOverwriteMode", "dynamic") \
                .parquet(s3_path)

print("Compactación de capa SILVER completada exitosamente.")
