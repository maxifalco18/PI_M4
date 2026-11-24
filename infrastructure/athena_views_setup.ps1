<#
.SYNOPSIS
Script para crear Vistas de Athena que responden preguntas de negocio.
#>

$Database = "business_gold"
Write-Host "Iniciando despliegue de Vistas de Athena en $Database..." -ForegroundColor Cyan

# 1. Ventas por Región (Hito 3/4)
$View1 = "CREATE OR REPLACE VIEW business_gold.v_sales_by_region AS 
SELECT customer_state, year, month, total_orders 
FROM business_gold.gold_sales_by_region;"

# 2. Ventas por Categoría y Tiempo (OBT)
$View2 = "CREATE OR REPLACE VIEW business_gold.v_sales_by_category AS 
SELECT product_category_name_english, year, month, total_revenue, total_items_sold 
FROM business_gold.gold_sales_by_category_time;"

# 3. Métricas de Pago (ROI/Profitability)
$View3 = "CREATE OR REPLACE VIEW business_gold.v_payment_metrics AS 
SELECT payment_type, year, month, total_revenue, total_transactions 
FROM business_gold.gold_gold_sales_by_payment;"

# Ejecución vía AWS CLI
$Queries = @($View1, $View2, $View3)

foreach ($Query in $Queries) {
    Write-Host "Ejecutando Q: $($Query.Substring(0, 50))..."
    aws athena start-query-execution --database $Database --query-string "$Query" --result-configuration "OutputLocation=s3://pi-m4-datalake-maxi/athena-results/"
}

Write-Host "Vistas de Athena desplegadas. Listas para QuickSight/Streamlit." -ForegroundColor Green
