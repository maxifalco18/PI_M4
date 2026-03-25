<#
.SYNOPSIS
Script para crear y disparar el Glue Crawler de la capa RAW.
#>

$CrawlerName = "raw_layer_discovery_crawler"
$DatabaseName = "raw_metadata"
$RoleName = "EC2_DataLake_Access_Role" # O un rol específico para Crawlers
$S3Path = "s3://pi-m4-datalake-maxi/raw/"

Write-Host "Creando base de datos $DatabaseName..." -ForegroundColor Cyan
aws glue create-database --database-input "{ \"Name\": \"$DatabaseName\" }"

Write-Host "Creando Glue Crawler: $CrawlerName..."
aws glue create-crawler `
    --name $CrawlerName `
    --role $RoleName `
    --database-name $DatabaseName `
    --targets "{ \"S3Targets\": [ { \"Path\": \"$S3Path\" } ] }" `
    --schedule "cron(0 0 * * ? *)" # Diario a medianoche

Write-Host "Disparando Crawler por primera vez..."
aws glue start-crawler --name $CrawlerName

Write-Host "Crawler configurado. Los metadatos de la capa RAW estarán en Athena pronto." -ForegroundColor Green
