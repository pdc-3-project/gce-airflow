import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.sensors.external_task import ExternalTaskSensor
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from plugins import slack

def extract_csv_from_gcs(**kwargs):
    execution_date = kwargs['execution_date']

    gcs_hook = GCSHook(gcp_conn_id='google_cloud_GCS')

    date_str = execution_date.strftime('%Y/%m/%d')
    bucket_name = 'pdc3project-raw-layer-bucket'
    object_name = f'source/airport_codes/{date_str}/airport_codes_{execution_date.strftime("%Y%m%d")}.csv'
    local_path = '/tmp/airport_codes.csv'

    gcs_hook.download(bucket_name, object_name, local_path)

def transform_csv_to_parquet(**kwargs):
    execution_date = kwargs['execution_date']

    local_csv_path = '/tmp/airport_codes.csv'
    local_parquet_path = '/tmp/airport_codes.parquet'

    df = pd.read_csv(local_csv_path)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, local_parquet_path)



with DAG(
    dag_id='airportCode_ELT',
    start_date=datetime(2024, 6, 1),
    schedule = '0 0 1 1 *',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': slack.on_failure_callback,
    }
) as dag:


    extract_task = PythonOperator(
        task_id='extract_csv_from_gcs',
        python_callable=extract_csv_from_gcs,
        provide_context=True,
        dag=dag,
    )
    
 
    transform_task = PythonOperator(
        task_id='transform_csv_to_parquet',
        python_callable=transform_csv_to_parquet,
        dag=dag,
    )

    load_to_stage_task = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src='/tmp/airport_codes.parquet',
        dst='source/airport_codes/{{ execution_date.strftime("%Y/%m/%d") }}/airport_codes_{{ execution_date.strftime("%Y%m%d") }}.parquet',
        bucket='pdc3project-stage-layer-bucket',
        gcp_conn_id='google_cloud_GCS',
    )

  
    load_to_bigquery_task = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket='pdc3project-stage-layer-bucket',
        #source_objects=['source/airport_codes/2024/06/13/airport_codes_20240613.parquet'],
        source_objects=['source/airport_codes/{{ execution_date.strftime("%Y/%m/%d") }}/airport_codes_{{ execution_date.strftime("%Y%m%d") }}.parquet'],
        destination_project_dataset_table='pdc3project.raw_data.airport_codes',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        schema_fields=[
            {'name': 'cityCode', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'cityEng', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'cityKor', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        gcp_conn_id='google_cloud_bigquery',
        dag=dag,
    )

    extract_task >> transform_task >> load_to_stage_task >> load_to_bigquery_task
