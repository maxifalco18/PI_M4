<#
.SYNOPSIS
Script para empaquetar la librería 'src' en un .zip para AWS Glue.
#>

$SourceDir = "src"
$ZipFile = "src.zip"
$S3Path = "s3://pi-m4-datalake-maxi/scripts/lib/src.zip"

Write-Host "Empaquetando la librería '$SourceDir'..." -ForegroundColor Cyan

# Eliminar zip previo si existe
if (Test-Path $ZipFile) { Remove-Item $ZipFile }

# Crear el nuevo zip (Recursivo)
Compress-Archive -Path "$SourceDir\*" -DestinationPath $ZipFile -Force

Write-Host "Cargando librería a S3: $S3Path..."
aws s3 cp $ZipFile $S3Path

Write-Host "Librería lista para usar como --extra-py-files en AWS Glue." -ForegroundColor Green
