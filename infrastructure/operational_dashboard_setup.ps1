<#
.SYNOPSIS
Script para configurar la infraestructura de monitoreo operacional del Data Lake.
#>

$Database = "operational_metadata"
Write-Host "Iniciando configuración de Tablero Operacional en $Database..." -ForegroundColor Cyan

# 1. Crear Base de Datos de Monitoreo
aws glue create-database --database-input "{ \"Name\": \"$Database\" }"

# 2. Vista de Latencia de Pipeline (Inferred from S3 timestamps)
$LatencyView = "CREATE OR REPLACE VIEW $Database.v_pipeline_latency AS 
SELECT 
    '$Database' as layer,
    count(*) as total_files,
    avg(date_diff('second', \"$path_timestamp\", current_timestamp)) as avg_latency_seconds
FROM business_gold.gold_sales_by_category_time;"

# 3. Vista de Control de Errores DQ (Audit trail)
$DQAuditView = "CREATE OR REPLACE VIEW $Database.v_dq_audit_summary AS 
SELECT 
    table_name,
    expectation,
    status,
    count(*) as total_occurrences
FROM business_gold.dq_results_log # Asumiendo un log persistente implementado en Fase 5
GROUP BY table_name, expectation, status;"

# Ejecución
$Queries = @($LatencyView, $DQAuditView)
foreach ($Query in $Queries) {
    aws athena start-query-execution --database $Database --query-string "$Query" --result-configuration "OutputLocation=s3://pi-m4-datalake-maxi/operational-dashboard/"
}

Write-Host "Infraestructura de monitoreo desplegada. Conectar CloudWatch/QuickSight para visualización." -ForegroundColor Green
