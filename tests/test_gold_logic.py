import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col

def test_gold_sales_aggregation_logic(spark):
    """Valida que la lógica de agregación en Gold sea correcta."""
    # Data de prueba (Silver mock)
    orders = [Row(order_id="O1", customer_id="C1", order_purchase_timestamp="2024-01-01 10:00:00")]
    items = [Row(order_id="O1", order_item_id=1, product_id="P1", price=100.0)]
    products = [Row(product_id="P1", product_category_name_english="electronics")]
    
    df_o = spark.createDataFrame(orders)
    df_i = spark.createDataFrame(items)
    df_p = spark.createDataFrame(products)
    
    # Simulación de la lógica en silver_to_gold.py
    df_gold = df_o.join(df_i, "order_id") \
                  .join(df_p, "product_id") \
                  .groupBy("product_category_name_english") \
                  .agg({"price": "sum", "order_id": "count"})
                  
    result = df_gold.first()
    
    # Validaciones
    assert result["sum(price)"] == 100.0
    assert result["count(order_id)"] == 1
    assert result["product_category_name_english"] == "electronics"

def test_lambda_union_logic(spark):
    """Valida la unión entre Batch y Streaming."""
    batch = [Row(order_id="BATCH_1")]
    streaming = [Row(order_id="STREAM_1")]
    
    df_b = spark.createDataFrame(batch)
    df_s = spark.createDataFrame(streaming)
    
    df_unified = df_b.unionByName(df_s)
    
    assert df_unified.count() == 2
    assert "order_id" in df_unified.columns
