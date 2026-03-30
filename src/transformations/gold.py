from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, sum as db_sum, count, col, current_date, datediff, max as db_max, min as db_min, avg, year as db_year, month as db_month, coalesce
from src.components.dq.validator import DataQualityValidator

def unify_lambda_orders(df_batch: DataFrame, df_streaming: DataFrame = None) -> DataFrame:
    """Unifies batch and streaming data for the Lambda architecture."""
    if df_streaming is not None:
        if "event_timestamp" in df_streaming.columns and "order_purchase_timestamp" not in df_streaming.columns:
             df_streaming = df_streaming.withColumn("order_purchase_timestamp", col("event_timestamp"))
        df_unified = df_batch.unionByName(df_streaming, allowMissingColumns=True)
    else:
        df_unified = df_batch
    
    # Ensure year and month exist for streaming merged rows
    if "year" in df_unified.columns and "order_purchase_timestamp" in df_unified.columns:
        df_unified = df_unified.withColumn("year", coalesce(col("year"), db_year(col("order_purchase_timestamp"))))
        df_unified = df_unified.withColumn("month", coalesce(col("month"), db_month(col("order_purchase_timestamp"))))
        
    # Deduplication using order_id as unique key
    df_deduped = df_unified.dropDuplicates(["order_id"])
    
    # DQ GATE: Hard gate for unified data
    dq = DataQualityValidator(df_deduped, "UNIFIED_ORDERS_GOLD")
    dq.expect_column_values_to_not_be_null("order_id") \
      .expect_column_values_to_be_unique("order_id")
    dq.validate(halt_on_fail=True)
    
    # Ensure order_value column is always present for safe downstream joins
    from pyspark.sql.functions import lit
    from pyspark.sql.types import DoubleType
    if "order_value" not in df_deduped.columns:
        df_deduped = df_deduped.withColumn("order_value", lit(None).cast(DoubleType()))
        
    return df_deduped

def transform_sales_by_category(df_items: DataFrame, df_products: DataFrame, df_orders: DataFrame) -> DataFrame:
    """Business logic: Sales by category and time (OBT)."""
    df_joined = df_items.join(broadcast(df_products), "product_id") \
                        .join(df_orders.select("order_id", "year", "month"), "order_id")
    return df_joined.groupBy("product_category_name", "year", "month") \
                    .agg(
                        db_sum("price").alias("total_revenue"),
                        count("order_item_id").alias("total_items_sold")
                    )

def transform_sales_by_region(df_orders: DataFrame, df_customers: DataFrame) -> DataFrame:
    """Business logic: Sales by region (customer state)."""
    return df_orders.join(broadcast(df_customers), "customer_id") \
                    .groupBy("customer_state", "year", "month") \
                    .agg(count("order_id").alias("total_orders"))

def transform_sales_by_payment(df_payments: DataFrame, df_orders: DataFrame) -> DataFrame:
    """Business logic: Sales by payment method."""
    return df_payments.join(df_orders, "order_id") \
                      .groupBy("payment_type", "year", "month") \
                      .agg(db_sum("payment_value").alias("total_revenue"),
                           count("order_id").alias("total_transactions"))

def transform_customer_segmentation(df_orders: DataFrame, df_customers: DataFrame, df_order_values: DataFrame) -> DataFrame:
    """Business logic: Customer Segmentation (RFM Basic)."""
    df_val = df_order_values.withColumnRenamed("order_value", "batch_order_value")
    df_orders_valued = df_orders.join(df_val, "order_id", "left") \
        .withColumn("final_order_value", coalesce(col("batch_order_value"), col("order_value"), lit(0.0)))
        
    df_customer_metrics = df_orders_valued.join(df_customers, "customer_id") \
        .groupBy("customer_unique_id") \
        .agg(
            db_max("order_purchase_timestamp").alias("last_purchase_date"),
            count("order_id").alias("frequency"),
            db_sum("final_order_value").alias("monetary")
        ) \
        .withColumn("recency_days", datediff(current_date(), col("last_purchase_date")))
    return df_customer_metrics

def transform_order_value_metrics(df_order_values: DataFrame, df_orders: DataFrame) -> DataFrame:
    """Business logic: Order Value Metrics."""
    df_val = df_order_values.withColumnRenamed("order_value", "batch_order_value")
    return df_orders.join(df_val, "order_id", "left") \
                    .withColumn("final_order_value", coalesce(col("batch_order_value"), col("order_value"), lit(0.0))) \
                    .groupBy("year", "month") \
                    .agg(
                        avg("final_order_value").alias("average_ticket"),
                        db_max("final_order_value").alias("max_ticket"),
                        db_min("final_order_value").alias("min_ticket"),
                        count("order_id").alias("total_orders")
                    )
