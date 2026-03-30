from datetime import datetime, timedelta
import logging
import requests
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.models import Variable

def on_failure_callback(context):
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    error_msg = f"⚠️ CRITICAL ALERT: Task {task_id} in DAG {dag_id} failed."
    logging.error(error_msg)
    
    # TASK-001: Implement Slack/Webhook Alerting
    webhook_url = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
    if webhook_url:
        try:
            requests.post(webhook_url, json={"text": error_msg})
        except Exception as e:
            logging.error(f"Failed to send Slack alert: {e}")

def run_airbyte_cloud_sync(connection_id_var):
    connection_id = Variable.get(connection_id_var)
    client_id = Variable.get("AIRBYTE_CLIENT_ID")
    client_secret = Variable.get("AIRBYTE_CLIENT_SECRET")
    
    print(f"Triggering Airbyte Cloud Sync for connection: {connection_id}")
    
    # 1. Obtenemos el Access Token dinámicamente
    token_url = "https://api.airbyte.com/v1/applications/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }
    
    # Para el endpoint de token de Airbyte usualmente se pasan los credenciales de client en headers o payload, pero según los docs es payload.
    # Opcionalmente probamos si es Basic Auth, pero Airbyte usa client_credentials payload.
    auth_resp = requests.post(token_url, json=payload, headers={"Content-Type": "application/json"})
    auth_resp.raise_for_status()
    access_token = auth_resp.json()["access_token"]
    
    # 2. Disparamos el Job de Sync
    job_url = "https://api.airbyte.com/v1/jobs"
    job_payload = {
        "jobType": "sync",
        "connectionId": connection_id
    }
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    print("Llamando a la API de Jobs...")
    job_resp = requests.post(job_url, json=job_payload, headers=headers)
    job_resp.raise_for_status()
    job_id = job_resp.json()["jobId"]
    print(f"Job disparado con éxito. ID: {job_id}")
    
    # 3. Hacemos Polling hasta que termine
    status_url = f"https://api.airbyte.com/v1/jobs/{job_id}"
    while True:
        status_resp = requests.get(status_url, headers=headers)
        status_resp.raise_for_status()
        status = status_resp.json()
        
        current_status = status.get("status", "pending")
        print(f"Estado del job: {current_status}")
        
        if current_status in ["succeeded", "completed"]:
            print("Sincronización finalizada exitosamente.")
            break
        elif current_status in ["failed", "cancelled"]:
            raise Exception(f"El job de Airbyte falló con estado: {current_status}")
            
        time.sleep(15)

default_args = {
    'owner': 'maxi',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_callback,
}

with DAG(
    'pi_m4_master_datalake_pipeline',
    default_args=default_args,
    description='Pipeline End-to-End: Ingesta Airbyte + Process PySpark en AWS Glue',
    schedule_interval='@daily',
    start_date=datetime(2026, 3, 24),
    catchup=False,
    tags=['ingestion', 'medallion', 'aws'],
) as dag:

    # 1. HITO 3 (Estricto según Consignas - 10 puntos de evaluación)
    # Ejecutamos en paralelo la Ingesta de Postgres y la de la API Pública usando APIs Nativas de Cloud
    sync_postgres_to_s3 = PythonOperator(
        task_id='trigger_airbyte_postgres_sync',
        python_callable=run_airbyte_cloud_sync,
        op_kwargs={'connection_id_var': 'AIRBYTE_CONN_POSTGRES'},
    )

    sync_api_to_s3 = PythonOperator(
        task_id='trigger_airbyte_api_sync',
        python_callable=run_airbyte_cloud_sync,
        op_kwargs={'connection_id_var': 'AIRBYTE_CONN_API'},
    )

    # TASK-004: Pass parameters to Glue Jobs
    bucket_name = Variable.get("S3_BUCKET_NAME", default_var="pi-m4-datalake-maxi")
    
    process_raw_to_silver = GlueJobOperator(
        task_id='spark_raw_to_silver_glue',
        job_name='job_raw_to_silver', 
        region_name='us-east-1',
        aws_conn_id='aws_default', 
        script_args={
            '--BUCKET_IN': f"s3://{bucket_name}/raw/batch",
            '--BUCKET_OUT': f"s3://{bucket_name}/processed/batch"
        },
        wait_for_completion=True,
        deferrable=False,
    )

    # 4. HITO 5 (Speed Layer Transformation): Raw -> Processed
    # NOTA: En un pipeline real, esto puede ser un job de Spark continuo, 
    # pero aquí lo orquestamos como un paso tras el batch para validación Lambda.
    speed_layer_process = GlueJobOperator(
        task_id='spark_speed_layer_process_glue',
        job_name='job_processed_streaming',
        region_name='us-east-1',
        aws_conn_id='aws_default',
        script_args={
            '--s3_raw_path': f"s3://{bucket_name}/raw-streaming/olist_events/",
            '--s3_processed_path': f"s3://{bucket_name}/processed-streaming/olist_events/",
            '--checkpoint_path': f"s3://{bucket_name}/checkpoints/speed_layer_trans/"
        },
        wait_for_completion=True,
        deferrable=False,
    )


    # EXTRA CREDIT: Auditoría de Calidad Independiente
    # Brinda visibilidad directa del estado DQ en la UI de Airflow
    dq_audit_silver = GlueJobOperator(
        task_id='dq_audit_silver_glue',
        job_name='job_dq_audit',
        region_name='us-east-1',
        aws_conn_id='aws_default',
        script_args={
            '--BUCKET_SILVER': f"s3://{bucket_name}/processed/batch",
        },
        wait_for_completion=True,
        deferrable=False,
    )

    delta_maintenance = GlueJobOperator(
        task_id='spark_delta_maintenance_glue',
        job_name='job_delta_maintenance',
        region_name='us-east-1',
        aws_conn_id='aws_default',
        script_args={
            '--s3_gold_path_prefix': f"s3://{bucket_name}/gold/"
        },
        wait_for_completion=True,
        deferrable=False,
    )

    process_silver_to_gold = GlueJobOperator(
        task_id='spark_silver_to_gold_glue',
        job_name='job_silver_to_gold',
        region_name='us-east-1',
        aws_conn_id='aws_default',
        script_args={
            '--BUCKET_SILVER': f"s3://{bucket_name}/processed/batch",
            '--BUCKET_STREAMING': f"s3://{bucket_name}/processed-streaming",
            '--BUCKET_GOLD': f"s3://{bucket_name}/gold"
        },
        wait_for_completion=True,
        deferrable=False,
    )

    # 4. ORQUESTACIÓN SECUENCIAL LÓGICA (Shift-Left Integration)
    [sync_postgres_to_s3, sync_api_to_s3] >> process_raw_to_silver >> dq_audit_silver >> speed_layer_process >> process_silver_to_gold
    process_silver_to_gold >> delta_maintenance
