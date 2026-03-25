# AWS KMS Encryption Guide for PIM4 Data Lake

Este documento detalla la implementación de cifrado de nivel empresarial (KMS CMK) para las capas Silver y Gold del Data Lake.

## 1. Creación de la Key (CMK)
Ejecutar el siguiente comando para generar la clave maestra:
```powershell
aws kms create-key --description "PIM4 Data Lake Encryption Key" --tags TagKey=Project,TagValue=PIM4
```

## 2. Aplicación a Buckets S3
Configurar el bucket de Gold para usar la nueva clave:
```powershell
aws s3api put-bucket-encryption `
    --bucket pi-m4-datalake-maxi `
    --server-side-encryption-configuration '{
        "Rules": [
            {
                "ApplyServerSideEncryptionByDefault": {
                    "SSEAlgorithm": "aws:kms",
                    "KMSMasterKeyID": "arn:aws:kms:us-east-1:ACCOUNT_ID:key/KEY_ID"
                }
            }
        ]
    }'
```

## 3. Permisos de IAM
El rol de Glue (`EC2_DataLake_Access_Role`) debe tener permisos en la Key:
- `kms:Decrypt`
- `kms:GenerateDataKey`
- `kms:DescribeKey`

## 4. Re-encriptación de Datos Existentes
Para datos ya cargados con SSE-S3, se recomienda ejecutar el script `kms_s3_reencryption.ps1`.
