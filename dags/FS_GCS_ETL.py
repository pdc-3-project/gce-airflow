import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.sensors.external_task import ExternalTaskSensor
from plugins import slack

def extract_csv_from_gcs(**kwargs):
    execution_date = kwargs['execution_date']

    gcs_hook = GCSHook(gcp_conn_id='google_cloud_GCS')

    date_str = execution_date.strftime('%Y/%m/%d')
    bucket_name = 'pdc3project-landing-zone-bucket'
    object_name = f'source/flight_data/{date_str}/flight_data_{execution_date.strftime("%Y%m%d")}.csv'
    local_path = '/tmp/flight_data.csv'

    gcs_hook.download(bucket_name, object_name, local_path)

def transform_csv_to_parquet(**kwargs):
    execution_date = kwargs['execution_date']
    local_csv_path = '/tmp/flight_data.csv'
    df = pd.read_csv(local_csv_path)
    
    df['AIRLINE_KOREAN'] = df['AIRLINE_KOREAN'].astype(str)
    df['AIRPORT'] = df['AIRPORT'].astype(str)
    df['ARRIVED_KOR'] = df['ARRIVED_KOR'].astype(str)
    df['BOARDING_KOR'] = df['BOARDING_KOR'].astype(str)
    df['FLIGHT_DATE'] = pd.to_datetime(df['FLIGHT_DATE'], format='%Y%m%d').dt.date
    df['IO'] = df['IO'].astype(str)
    df['LINE'] = df['LINE'].astype(str)
    df['RMK_KOR'] = df['RMK_KOR'].astype(str)
    df['STD'] = pd.to_numeric(df['STD'], errors='coerce').fillna(0).astype('int64')
    df['ETD'] = pd.to_numeric(df['ETD'], errors='coerce').fillna(0).astype('int64')
    df['UFID'] = df['UFID'].astype(str)

    incheon_data = df[(df['BOARDING_KOR'] == '인천') | (df['ARRIVED_KOR'] == '인천')]
    gimpo_data = df[(df['BOARDING_KOR'] == '서울/김포') | (df['ARRIVED_KOR'] == '서울/김포')]
    jeju_data = df[(df['BOARDING_KOR'] == '제주') | (df['ARRIVED_KOR'] == '제주')]

    incheon_data['target_airport'] = '인천'
    gimpo_data['target_airport'] = '서울/김포'
    jeju_data['target_airport'] = '제주'

    combined_data = pd.concat([incheon_data, gimpo_data, jeju_data], ignore_index=True)

    parquet_path = '/tmp/flight_data.parquet'
    table = pa.Table.from_pandas(combined_data)
    pq.write_table(table, parquet_path)
    
    kwargs['ti'].xcom_push(key='parquet_path', value=parquet_path)

def upload_to_gcs(**kwargs):
    execution_date = kwargs['execution_date']
    parquet_path = kwargs['ti'].xcom_pull(key='parquet_path', task_ids='transform_csv_to_parquet')
    
    bucket = 'pdc3project-stage-layer-bucket'
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_GCS')
    dst_path = f'source/flight_data/{execution_date.strftime("%Y/%m/%d")}/flight_data_{execution_date.strftime("%Y%m%d")}.parquet'
    
    gcs_hook.upload(bucket_name=bucket,
                    object_name=dst_path,
                    filename=parquet_path)
    
with DAG(
    dag_id='FS_gcs_to_bigquery',
    start_date=datetime(2024, 6, 1),
    schedule_interval='0 0 * * *',
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
    )
    
    transform_task = PythonOperator(
        task_id='transform_csv_to_parquet',
        python_callable=transform_csv_to_parquet,
        provide_context=True,
    )
    
    upload_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        provide_context=True,
    )
    
    load_to_bigquery_task = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket='pdc3project-stage-layer-bucket',
        source_objects=[f'source/flight_data/{{{{ execution_date.strftime("%Y/%m/%d") }}}}/flight_data_{{{{ execution_date.strftime("%Y%m%d") }}}}.parquet'],
        destination_project_dataset_table='pdc3project.raw_data.flight_data',
        source_format='PARQUET',
        write_disposition='WRITE_APPEND',
        schema_fields=[
            {'name': 'AIRLINE_KOREAN', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'AIRPORT', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'ARRIVED_KOR', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'BOARDING_KOR', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'FLIGHT_DATE', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'IO', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'LINE', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'RMK_KOR', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'STD', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'ETD', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'UFID', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'DELAY_TIME', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'target_airport', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        gcp_conn_id='google_cloud_bigquery',
        dag=dag,
    )

    extract_task >> transform_task >> upload_task >> load_to_bigquery_task
