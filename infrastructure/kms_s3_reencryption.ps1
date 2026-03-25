<#
.SYNOPSIS
Script para re-encriptar objetos existentes en S3 usando una nueva clave KMS.
#>

param (
    [Parameter(Mandatory=$true)]
    [string]$BucketName,
    
    [Parameter(Mandatory=$true)]
    [string]$KmsKeyId
)

Write-Host "Iniciando re-encriptación KMS para el bucket $BucketName..." -ForegroundColor Cyan

# Listar y re-copiar objetos (esto aplica el nuevo cifrado por defecto del bucket)
$objects = aws s3api list-objects-v2 --bucket $BucketName --query 'Contents[].Key' --output text

foreach ($key in $objects) {
    Write-Host "Procesando $key..."
    aws s3 cp "s3://$BucketName/$key" "s3://$BucketName/$key" --sse aws:kms --sse-kms-key-id $KmsKeyId
}

Write-Host "Re-encriptación completada." -ForegroundColor Green
