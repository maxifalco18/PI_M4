from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.models import Variable
from datetime import datetime, timedelta

# Default arguments for the Backfill DAG
default_args = {
    'owner': 'senior_data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'backfill_historical_pipeline',
    default_args=default_args,
    description='Re-procesamiento histórico de Silver y Gold por rangos de fecha',
    schedule_interval=None, # Solo ejecución manual (Trigger with Config)
    tags=['operaciones', 'mantenimiento'],
    catchup=False
) as dag:

    # 1. Obtención de parámetros de ejecución (Trigger with config: {"start_date": "2024-01-01", "end_date": "2024-01-31"})
    # Estos se pasan a Glue para filtrar el Raw layer si es necesario.
    s3_bucket = Variable.get("S3_BUCKET_NAME", default_var="pi-m4-datalake-maxi")

    backfill_silver = GlueJobOperator(
        task_id='backfill_silver_partitions',
        job_name='job_raw_to_silver',
        region_name='us-east-1',
        script_args={
            '--BUCKET_IN': f"s3://{s3_bucket}/raw-batch",
            '--BUCKET_OUT': f"s3://{s3_bucket}/processed/batch",
            '--START_DATE': "{{ dag_run.conf['start_date'] if dag_run else '1900-01-01' }}",
            '--END_DATE': "{{ dag_run.conf['end_date'] if dag_run else '9999-12-31' }}"
        },
        wait_for_completion=True,
    )

    backfill_gold = GlueJobOperator(
        task_id='backfill_gold_partitions',
        job_name='job_silver_to_gold',
        region_name='us-east-1',
        script_args={
            '--BUCKET_SILVER': f"s3://{s3_bucket}/processed/batch",
            '--BUCKET_STREAMING': f"s3://{s3_bucket}/processed-streaming",
            '--BUCKET_GOLD': f"s3://{s3_bucket}/gold"
        },
        wait_for_completion=True,
    )

    backfill_silver >> backfill_gold
