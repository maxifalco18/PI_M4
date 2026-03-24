<#
.SYNOPSIS
Script para automatizar la creación de la infraestructura base del Hito 1 (S3 e IAM) usando AWS CLI.

.DESCRIPTION
Este script crea:
1. Un bucket de S3 con las carpetas /raw, /silver y /gold.
2. Un Rol de IAM para EC2 con políticas de menor privilegio sobre el bucket creado.
3. Un Instance Profile para asociar el rol a la EC2.

.PARAMETER BucketName
El nombre único para el bucket, por ejemplo: pi-henry-datalake-tuapellido
#>

param (
    [Parameter(Mandatory=$true)]
    [string]$BucketName,

    [string]$Region = "us-east-1"
)

$ErrorActionPreference = "Stop"

Write-Host "Iniciando aprovisionamiento del Hito 1..." -ForegroundColor Cyan

# 1. Crear el Bucket S3
Write-Host "1. Creando bucket de S3: $BucketName en $Region..."
aws s3api create-bucket --bucket $BucketName --region $Region

# Crear carpetas "lógicas" en S3
Write-Host "Creando carpetas raw/, silver/ y gold/..."
aws s3api put-object --bucket $BucketName --key raw/
aws s3api put-object --bucket $BucketName --key silver/
aws s3api put-object --bucket $BucketName --key gold/

Write-Host "Habilitando Server-Side Encryption (SSE-S3)..."
aws s3api put-bucket-encryption `
    --bucket $BucketName `
    --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

# 2. Crear Rol IAM e Instance Profile
$RoleName = "EC2_DataLake_Access_Role"
$PolicyName = "S3_DataLake_Access_Policy"

Write-Host "2. Creando Rol IAM: $RoleName..."
$TrustPolicy = @"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
"@
Set-Content -Path "trust-policy.json" -Value $TrustPolicy
aws iam create-role --role-name $RoleName --assume-role-policy-document file://trust-policy.json
Remove-Item -Path "trust-policy.json"

Write-Host "Creando política de permisos S3: $PolicyName..."
$AccessPolicy = @"
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::$BucketName"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject"
            ],
            "Resource": "arn:aws:s3:::$BucketName/*"
        }
    ]
}
"@
Set-Content -Path "access-policy.json" -Value $AccessPolicy
$PolicyArn = (aws iam create-policy --policy-name $PolicyName --policy-document file://access-policy.json --query 'Policy.Arn' --output text)
Remove-Item -Path "access-policy.json"

Write-Host "Asociando política al rol..."
aws iam attach-role-policy --role-name $RoleName --policy-arn $PolicyArn

Write-Host "Creando Instance Profile (para asignarlo luego a la EC2)..."
$InstanceProfileName = "EC2_DataLake_Profile"
try {
    aws iam create-instance-profile --instance-profile-name $InstanceProfileName
} catch {
    Write-Host "El Instance Profile ya existe o hubo un problema al crearlo." -ForegroundColor Yellow
}

aws iam add-role-to-instance-profile --instance-profile-name $InstanceProfileName --role-name $RoleName

Write-Host "`n✅ Aprovisionamiento completado exitosamente." -ForegroundColor Green
Write-Host "Bucket S3 creado: $BucketName"
Write-Host "Instance Profile creado: $InstanceProfileName (Úsalo cuando crees la instancia EC2)"
