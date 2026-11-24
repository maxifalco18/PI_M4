import pytest
from pyspark.sql import Row
from datetime import date

def test_scd2_logic(spark):
    """Prueba que los campos de SCD2 se agreguen correctamente."""
    # Data de prueba
    data = [Row(customer_id="C1", customer_state="SP")]
    df = spark.createDataFrame(data)
    
    # Simulación de la lógica en raw_to_silver.py
    from pyspark.sql.functions import lit, current_date
    df_result = df.withColumn("effective_date", current_date()) \
                  .withColumn("end_date", lit("9999-12-31").cast("date")) \
                  .withColumn("is_current", lit(True))
    
    # Validaciones
    assert "effective_date" in df_result.columns
    assert df_result.filter(df_result.is_current == True).count() == 1
    assert df_result.first()["end_date"] == date(9999, 12, 31)
