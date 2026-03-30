import sys
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions

# Obtain correct parameters using AWS Glue utility
try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_gold_path_prefix'])
except Exception as e:
    # Fallback to defaults if run locally or args are missing
    args = {'JOB_NAME': 'local', 's3_gold_path_prefix': 's3://pi-m4-datalake-maxi/gold/'}

gold_prefix = args['s3_gold_path_prefix']
# Derive the path for the real delta table from the gold prefix
# Example: s3://pi-m4-datalake-maxi/gold/ -> s3://pi-m4-datalake-maxi/processed/batch/dim_customers/
bucket_name = gold_prefix.replace("s3://", "").split("/")[0]
silver_customers_path = f"s3://{bucket_name}/processed/batch/dim_customers/"

spark = SparkSession.builder \
    .appName("DeltaLakeMaintenance") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print(f"Iniciando mantenimiento Delta en: {silver_customers_path}")

try:
    print("--- Optimizando tabla: dim_customers (SCD2) ---")
    # Execute maintenance natively on S3 path for Delta Lake
    spark.sql(f"OPTIMIZE delta.`{silver_customers_path}`")
    spark.sql(f"VACUUM delta.`{silver_customers_path}` RETAIN 168 HOURS")
    print("Mantenimiento Delta completado exitosamente.")
except Exception as e:
    print(f"Advertencia durante el mantenimiento Delta (la tabla puede no existir o no requiere vaciado): {e}")
