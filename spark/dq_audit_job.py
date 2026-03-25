import sys
import os
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Environment Setup
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from src.components.dq.validator import DataQualityValidator

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_SILVER'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket_silver = args['BUCKET_SILVER']

# DQ AUDIT: DIM_CUSTOMERS (Hito 2/Extra Credit)
df_customers = spark.read.parquet(f"{bucket_silver}/dim_customers/")
validator = DataQualityValidator(df_customers, "AUDIT_CUSTOMERS")
validator.expect_column_values_to_not_be_null("customer_id") \
         .expect_column_values_to_be_unique("customer_id")
validator.validate(halt_on_fail=True)

# DQ AUDIT: FACT_ORDERS
df_orders = spark.read.parquet(f"{bucket_silver}/fact_orders/")
validator_orders = DataQualityValidator(df_orders, "AUDIT_ORDERS")
validator_orders.expect_column_values_to_not_be_null("order_id")
validator_orders.validate(halt_on_fail=True)

job.commit()
print("¡Auditoría DQ completada exitosamente!")
