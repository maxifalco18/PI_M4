from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.models import Variable

def on_failure_callback(context):
    task_id = context.get('task_instance').task_id
    logging.error(f"⚠️ CRITICAL ALERT: Task {task_id} failed. Please review the logs.")

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
    # Ejecutamos en paralelo la Ingesta de Postgres y la de la API Pública
    sync_postgres_to_s3 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_postgres_sync',
        airbyte_conn_id='airbyte_default',
        connection_id="{{ var.value.get('AIRBYTE_CONN_POSTGRES', 'PLACEHOLDER_POSTGRES_ID') }}",
        asynchronous=False, 
    )

    sync_api_to_s3 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_api_sync',
        airbyte_conn_id='airbyte_default',
        connection_id="{{ var.value.get('AIRBYTE_CONN_API', 'PLACEHOLDER_API_ID') }}",
        asynchronous=False, 
    )

    # 2. HITO 4 (Capa Silver): Ejecuta el Job Process en AWS Glue
    process_raw_to_silver = GlueJobOperator(
        task_id='spark_raw_to_silver_glue',
        job_name='job_raw_to_silver', 
        region_name='us-east-1',
        aws_conn_id='aws_default', 
        wait_for_completion=True,
        deferrable=True,
    )

    # 3. HITO 4 (Capa Gold): Genera las Tablas de Negocio en formato Parquet
    process_silver_to_gold = GlueJobOperator(
        task_id='spark_silver_to_gold_glue',
        job_name='job_silver_to_gold',
        region_name='us-east-1',
        aws_conn_id='aws_default',
        wait_for_completion=True,
        deferrable=True,
    )

    # 4. ORQUESTACIÓN SECUENCIAL LÓGICA (Shift-Left Integration)
    [sync_postgres_to_s3, sync_api_to_s3] >> process_raw_to_silver >> process_silver_to_gold
