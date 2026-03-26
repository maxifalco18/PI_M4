from pyspark.sql import DataFrame
from pyspark.sql.functions import year, month, lit, current_date
from src.components.dq.validator import DataQualityValidator

def apply_dq_checks(df: DataFrame, table_name: str, pk_col: str) -> DataFrame:
    """Encapsulates DQ logic using the modular framework."""
    validator = DataQualityValidator(df, table_name)
    validator.expect_column_values_to_not_be_null(pk_col, threshold=0.01) \
             .expect_column_values_to_be_unique(pk_col)
    validator.validate(halt_on_fail=False)
    return df

def transform_geolocation(df: DataFrame) -> DataFrame:
    """Transformation logic for DIM_GEOLOCATION."""
    return df.dropDuplicates(['geolocation_zip_code_prefix'])

def transform_orders(df: DataFrame) -> DataFrame:
    """Transformation logic for FACT_ORDERS."""
    df = apply_dq_checks(df, "FACT_ORDERS", "order_id")
    return df.withColumn("year", year("order_purchase_timestamp")) \
             .withColumn("month", month("order_purchase_timestamp"))

def transform_payments(df: DataFrame) -> DataFrame:
    """Transformation logic for FACT_PAYMENTS."""
    return df # Currently just the raw data before partitioning

def transform_customers(df: DataFrame) -> DataFrame:
    """
    Transformation logic for DIM_CUSTOMERS with SCD Type 2 metadata.
    Note: The actual MERGE logic to handle versioning is implemented in the Glue Job 
    entry point using the metadata added here.
    """
    df = apply_dq_checks(df, "DIM_CUSTOMERS", "customer_id")
    df = df.dropDuplicates(['customer_id'])
    
    # Adding SCD Type 2 metadata
    return df.withColumn("effective_date", current_date()) \
             .withColumn("end_date", lit("9999-12-31").cast("date")) \
             .withColumn("is_current", lit(True))

def transform_sellers(df: DataFrame) -> DataFrame:
    """Transformation logic for DIM_SELLERS."""
    return df.dropDuplicates(['seller_id'])

def transform_products(df: DataFrame) -> DataFrame:
    """Transformation logic for DIM_PRODUCTS."""
    return df.dropDuplicates(['product_id'])

def transform_order_items(df: DataFrame) -> DataFrame:
    """Transformation logic for FACT_ORDER_ITEMS."""
    return df.dropDuplicates(['order_id', 'order_item_id'])

def transform_order_reviews(df: DataFrame) -> DataFrame:
    """Transformation logic for FACT_ORDER_REVIEWS."""
    return df.dropDuplicates(['review_id'])
