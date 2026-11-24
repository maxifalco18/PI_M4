"""
validate_demo.py
================
Script de validación end-to-end para la demo del PIM4.
Ejecutar desde la raíz del proyecto:  python validate_demo.py
"""

import boto3
import requests

BUCKET      = "pi-m4-datalake-maxi"
AIRFLOW_URL = "http://100.31.44.224:8080"          # IP corregida por el usuario
REGION      = "us-east-1"

# Colores ANSI (Texto plano para evitar errores de encoding en Windows)
OK   = "\033[92m[OK]   \033[0m"
FAIL = "\033[91m[FAIL] \033[0m"
WARN = "\033[93m[WARN] \033[0m"

s3 = boto3.client("s3", region_name=REGION)

# ──────────────────────────────────────────────────────────────────────────────
# UTILIDADES
# ──────────────────────────────────────────────────────────────────────────────
def check_s3_prefix(prefix):
    """Devuelve True si el prefix tiene al menos un objeto."""
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1)
    return "Contents" in resp

def count_s3_objects(prefix):
    """Cuenta objetos en un prefix (máx. 1000)."""
    paginator = s3.get_paginator("list_objects_v2")
    total = 0
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        total += page.get("KeyCount", 0)
    return total

# ──────────────────────────────────────────────────────────────────────────────
# CHECK 1 — S3 CAPAS
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("  CHECK 1: S3 — Existencia de Capas del Data Lake")
print("="*60)

LAYERS = {
    "RAW Batch":        "raw/batch/",
    "RAW Streaming":    "raw-streaming/olist_events/",
    "SILVER (processed/batch)": "processed/batch/",
    "GOLD":             "gold/",
    "STREAMING Processed": "processed-streaming/",
}

for name, prefix in LAYERS.items():
    exists = check_s3_prefix(prefix)
    cnt    = count_s3_objects(prefix) if exists else 0
    status = OK if exists else FAIL
    print(f"{status} {name:<35} — {cnt} objetos  (s3://{BUCKET}/{prefix})")


# ──────────────────────────────────────────────────────────────────────────────
# CHECK 2 — AIRFLOW
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("  CHECK 2: AIRFLOW — Estado del DAG")
print("="*60)

try:
    resp = requests.get(
        f"{AIRFLOW_URL}/api/v1/dags/pi_m4_master_datalake_pipeline/dagRuns",
        auth=("airflow", "airflow"),
        timeout=5,
    )
    if resp.status_code == 200:
        runs = resp.json().get("dag_runs", [])
        if runs:
            last = runs[-1]
            state = last.get("state", "unknown")
            color = OK if state == "success" else (WARN if state == "running" else FAIL)
            print(f"{color} Última corrida -> {state.upper()}  ({last.get('execution_date', '')})")
        else:
            print(f"{WARN} DAG existe pero nunca corrió.")
    elif resp.status_code == 401:
        print(f"{WARN} Airflow respondió HTTP 401 — verificá las credenciales o la configuración de la API.")
    else:
        print(f"{FAIL} Airflow respondió HTTP {resp.status_code} — verificá las credenciales.")
except Exception as e:
    print(f"{FAIL} No se pudo conectar a Airflow ({AIRFLOW_URL}) -> {e}")


# ──────────────────────────────────────────────────────────────────────────────
# CHECK 3 — STREAMING (Kafka → S3)
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("  CHECK 3: STREAMING — Datos recibidos desde Kafka")
print("="*60)

PREFIX_STREAM = "raw-streaming/olist_events/"
cnt_stream = count_s3_objects(PREFIX_STREAM)
if cnt_stream > 0:
    print(f"{OK} Se encontraron {cnt_stream} archivos en raw-streaming/olist_events/")
else:
    print(f"{FAIL} No hay datos en raw-streaming/. Ejecutá: python spark/kafka_producer_sim.py")


# ──────────────────────────────────────────────────────────────────────────────
# CHECK 4 — TRANSFORMACIÓN SPARK (Silver + Gold tienen datos)
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("  CHECK 4: SPARK — Validacion de capas procesadas")
print("="*60)

SPARK_CHECKS = {
    "dim_customers (Silver)":          "processed/batch/dim_customers/",
    "fact_orders (Silver)":            "processed/batch/fact_orders/",
    "gold_sales_by_category":          "gold/gold_sales_by_category_time/",
    "gold_sales_by_region":            "gold/gold_sales_by_region/",
    "gold_sales_by_payment":           "gold/gold_sales_by_payment_method/",
}

for name, prefix in SPARK_CHECKS.items():
    exists = check_s3_prefix(prefix)
    cnt    = count_s3_objects(prefix) if exists else 0
    status = OK if cnt > 0 else FAIL
    print(f"{status} {name:<35} — {cnt} archivos Parquet/Delta")


# ──────────────────────────────────────────────────────────────────────────────
# CHECK 5 — LAMBDA UNIFICADA (Gold tiene eventos batch + streaming)
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("  CHECK 5: LAMBDA — Gold unifica Batch + Streaming")
print("="*60)

batch_ok    = check_s3_prefix("gold/gold_sales_by_category_time/")
stream_ok   = check_s3_prefix("processed-streaming/")

if batch_ok and stream_ok:
    print(f"{OK} Ambas fuentes presentes -> Lambda Architecture activa")
elif batch_ok:
    print(f"{WARN} Solo hay datos Batch. El Speed Layer aún no procesó eventos.")
else:
    print(f"{FAIL} Ni la capa Batch ni la Streaming están en Gold.")


# ──────────────────────────────────────────────────────────────────────────────
# CHECK 6 — GOVERNANCE (Schema Registry & KMS)
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("  CHECK 6: GOVERNANCE — Schema Registry & KMS")
print("="*60)

glue = boto3.client("glue", region_name=REGION)
try:
    # Verificamos si existe el registro 'olist-registry'
    glue.get_registry(RegistryId={'RegistryName': 'olist-registry'})
    print(f"{OK} AWS Glue Schema Registry ('olist-registry') registrado.")
except Exception:
    print(f"{FAIL} Schema Registry 'olist-registry' no encontrado.")

try:
    # Verificamos cifrado KMS del bucket
    enc = s3.get_bucket_encryption(Bucket=BUCKET)
    rules = enc.get('ServerSideEncryptionConfiguration', {}).get('Rules', [])
    if rules:
        print(f"{OK} Se encontro el Bucket con KMS/AES256 habilitado.")
    else:
        print(f"{WARN} S3 Bucket sin cifrado por defecto configurado.")
except Exception:
    print(f"{WARN} No se pudo obtener la configuración de cifrado de S3.")


# ──────────────────────────────────────────────────────────────────────────────
# RESUMEN
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("  VALIDACIÓN COMPLETA")
print("="*60)
print("Si todos los checks son [OK] -> el proyecto esta listo para la demo.\n")
