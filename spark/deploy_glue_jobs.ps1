$ErrorActionPreference = "Stop"
$RoleName = "Rol_Glue_DataLake"

Write-Host "`nCreando AWS Glue Jobs (Reintento sin JSON problemático)..."
aws glue delete-job --job-name "job_raw_to_silver" 2>$null
aws glue delete-job --job-name "job_silver_to_gold" 2>$null

Write-Host "   Creando job: job_raw_to_silver..."
# The script was already uploaded to S3 in the previous successful step
aws glue create-job --name "job_raw_to_silver" `
    --role $RoleName `
    --command 'Name=glueetl,ScriptLocation=s3://pi-m4-datalake-maxi/scripts/raw_to_silver.py,PythonVersion=3' `
    --glue-version "4.0" `
    --worker-type "G.1X" `
    --number-of-workers 2 | Out-Null

Write-Host "   Creando job: job_silver_to_gold..."
aws glue create-job --name "job_silver_to_gold" `
    --role $RoleName `
    --command 'Name=glueetl,ScriptLocation=s3://pi-m4-datalake-maxi/scripts/silver_to_gold.py,PythonVersion=3' `
    --glue-version "4.0" `
    --worker-type "G.1X" `
    --number-of-workers 2 | Out-Null

Write-Host "EXITO! Los AWS Glue Jobs fueron creados correctamente."
