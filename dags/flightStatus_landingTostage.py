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
    
    df['FLIGHT_DATE'] = pd.to_datetime(df['FLIGHT_DATE'], format='%Y%m%d').dt.date

    incheon_data = df[(df['BOARDING_KOR'] == '인천') | (df['ARRIVED_KOR'] == '인천')]
    gimpo_data = df[(df['BOARDING_KOR'] == '서울/김포') | (df['ARRIVED_KOR'] == '서울/김포')]
    jeju_data = df[(df['BOARDING_KOR'] == '제주') | (df['ARRIVED_KOR'] == '제주')]

    parquet_files = {}

    if not incheon_data.empty:
        incheon_parquet_path = '/tmp/flight_data_incheon.parquet'
        table = pa.Table.from_pandas(incheon_data)
        pq.write_table(table, incheon_parquet_path)
        parquet_files['INCHEON'] = incheon_parquet_path

    if not gimpo_data.empty:
        gimpo_parquet_path = '/tmp/flight_data_gimpo.parquet'
        table = pa.Table.from_pandas(gimpo_data)
        pq.write_table(table, gimpo_parquet_path)
        parquet_files['GIMPO'] = gimpo_parquet_path

    if not jeju_data.empty:
        jeju_parquet_path = '/tmp/flight_data_jeju.parquet'
        table = pa.Table.from_pandas(jeju_data)
        pq.write_table(table, jeju_parquet_path)
        parquet_files['JEJU'] = jeju_parquet_path
    
    kwargs['ti'].xcom_push(key='parquet_files', value=parquet_files)
    

def upload_to_gcs(**kwargs):
    execution_date = kwargs['execution_date']
    parquet_files = kwargs['ti'].xcom_pull(key='parquet_files', task_ids='transform_csv_to_parquet')
    
    bucket = 'pdc3project-stage-layer-bucket'
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_GCS')

    for airport, local_parquet_path in parquet_files.items():
        dst_path = f'source/flight_data/{execution_date.strftime("%Y/%m/%d")}/flight_data_{execution_date.strftime("%Y%m%d")}_{airport}.parquet'
        
        gcs_hook.upload(bucket_name=bucket,
                        object_name=dst_path,
                        filename=local_parquet_path)
        
        
with DAG(
    dag_id='filghtStatus_ELT',
    start_date=datetime(2024, 6, 12),
    schedule_interval='0 0 * * *',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        #'on_failure_callback': slack.on_failure_callback,
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
    
    # sensor = ExternalTaskSensor(
    #     task_id='wait_for_api_fetch_completion',
    #     external_dag_id='flight_data_fetcher',
    #     external_task_id='upload_to_gcs_task', 
    #     execution_date_fn=lambda x: x,
    #     mode='reschedule',
    #     poke_interval=300,
    #     timeout=600,
    #     dag=dag
    # )

    # BigQuery 업로드 태스크를 공항별로 생성
    airports = ['INCHEON', 'GIMPO', 'JEJU']
    bq_tasks = []
    for airport in airports:
        bq_task = GCSToBigQueryOperator(
            task_id=f'load_{airport}_to_bigquery',
            bucket='pdc3project-stage-layer-bucket',
            source_objects=[f'source/flight_data/{{{{ execution_date.strftime("%Y/%m/%d") }}}}/flight_data_{{{{ execution_date.strftime("%Y%m%d") }}}}_{airport}.parquet'],
            destination_project_dataset_table=f'pdc3project.raw_data.flight_data_{airport}',
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
                {'name': 'ETD', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
                {'name': 'UFID', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'DELAY_TIME', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            ],
            gcp_conn_id='google_cloud_bigquery',
            dag=dag,
        )
        bq_tasks.append(bq_task)

    extract_task >> transform_task >> upload_task >> bq_tasks
