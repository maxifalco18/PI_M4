<#
.SYNOPSIS
Script para configurar AWS Lake Formation y registrar el bucket del Data Lake.
#>

param (
    [Parameter(Mandatory=$true)]
    [string]$BucketName,
    
    [string]$AdminRoleArn
)

Write-Host "Iniciando configuración de Lake Formation..." -ForegroundColor Cyan

# 1. Registrar el Bucket S3 como ubicación de datos
Write-Host "Registrando ubicación S3: $BucketName..."
aws lakeformation register-resource --resource-arn "arn:aws:s3:::$BucketName" --use-service-linked-role

# 2. Configurar el Administrador de Lake Formation (opcional si ya está configurado)
if ($AdminRoleArn) {
    Write-Host "Configurando administrador de LF: $AdminRoleArn..."
    aws lakeformation put-data-lake-settings --data-lake-settings "{\"DataLakeAdmins\": [{\"DataLabels\": [], \"PrincipalArn\": \"$AdminRoleArn\"}]}"
}

# 3. Otorgar permisos de lectura/escritura al Rol de la EC2 para el Catálogo
$EC2RoleArn = (aws iam get-role --role-name "EC2_DataLake_Access_Role" --query 'Role.Arn' --output text)
Write-Host "Otorgando permisos al rol: $EC2RoleArn..."

aws lakeformation grant-permissions `
    --principal "DataLabels=[],PrincipalArn=$EC2RoleArn" `
    --resource "{ \"Database\": { \"Name\": \"processed_silver\" } }" `
    --permissions "ALL"

aws lakeformation grant-permissions `
    --principal "DataLabels=[],PrincipalArn=$EC2RoleArn" `
    --resource "{ \"Database\": { \"Name\": \"business_gold\" } }" `
    --permissions "ALL"

Write-Host "Configuración de Lake Formation completada exitosamente." -ForegroundColor Green
