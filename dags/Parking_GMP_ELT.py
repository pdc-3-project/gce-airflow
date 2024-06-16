from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa
from plugins import slack
import pytz
import re


def convert_to_kst(execution_date):
    # UTC에서 한국 시간으로 변환
    execution_date_utc = execution_date.replace(tzinfo=pytz.UTC)
    execution_date_kst = execution_date_utc.astimezone(pytz.timezone('Asia/Seoul'))
    return execution_date_kst.strftime('%Y-%m-%d %H:%M:%S')

def set_kst_execution_date(**kwargs):
    # Airflow의 execution_date를 가져와서 한국 시간대로 변환 후 XCom에 저장
    execution_date = kwargs['execution_date']
    execution_date_kst = convert_to_kst(execution_date)
    ti = kwargs['ti']
    ti.xcom_push(key='execution_date_kst', value=execution_date_kst)

def extract_csv_from_gcs(execution_date_str, **kwargs):
    ti = kwargs['ti']
    execution_date_kst = ti.xcom_pull(task_ids='set_kst_execution_date', key='execution_date_kst')
    # Convert the pulled execution_date_kst string back to a datetime object
    execution_date_kst_dt = datetime.strptime(execution_date_kst, '%Y-%m-%d %H:%M:%S')
    formatted_day = execution_date_kst_dt.strftime("%d")
    formatted_timestamp = execution_date_kst_dt.strftime("%Y%m%d%H")
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_GCS')
    bucket_name = 'pdc3project-landing-zone-bucket'

    # GCS 객체의 prefix와 정규 표현식 패턴을 설정
    prefix = f'source/source_parkinglot/GMP_parking_data/2024/06/{formatted_day}'
    regex_pattern = f'GMP_parking_data_{formatted_timestamp}\d{{2}}.csv'

    # 지정된 GCS prefix에서 모든 파일 리스트를 가져옴
    objects = gcs_hook.list(bucket_name, prefix=prefix)

    # 정규 표현식 패턴과 일치하는 파일을 찾음
    matching_files = [obj for obj in objects if re.search(regex_pattern, obj)]

    if not matching_files:
        raise FileNotFoundError(f"No files matching pattern {regex_pattern} in {prefix}")

    # 첫 번째로 일치하는 파일을 다운로드
    object_name = matching_files[0]
    local_path = '/tmp/GMP_parking_data.csv'
    gcs_hook.download(bucket_name, object_name, local_path)


def transform_csv_to_parquet(execution_date_str, **kwargs):
    # 로컬에 저장된 CSV 파일 경로를 설정
    local_csv_path = '/tmp/GMP_parking_data.csv'
    df = pd.read_csv(local_csv_path)

    # Parquet 파일로 저장할 경로를 설정
    parquet_path = '/tmp/GMP_parking_data.parquet'
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_path)

    # 현재 태스크 인스턴스를 나타내는 task_instance 객체를 가져옴
    ti = kwargs['ti']

    # XCom을 사용하여 Parquet 파일 경로를 push(저장)
    ti.xcom_push(key='parquet_path', value=parquet_path)


def upload_to_gcs(execution_date_str, **kwargs):
    # 현재 태스크 인스턴스를 나타내는 task_instance 객체를 가져옴
    ti = kwargs['ti']
    execution_date_kst = ti.xcom_pull(task_ids='set_kst_execution_date', key='execution_date_kst')
    # Convert the pulled execution_date_kst string back to a datetime object
    execution_date_kst_dt = datetime.strptime(execution_date_kst, '%Y-%m-%d %H:%M:%S')
    formatted_day = execution_date_kst_dt.strftime("%d")
    formatted_timestamp = execution_date_kst_dt.strftime("%Y%m%d%H%M")
    # 이전 태스크에서 저장된 Parquet 파일 경로를 XCom을 통해 가져옴
    parquet_path = ti.xcom_pull(key='parquet_path', task_ids='transform_csv_to_parquet')

    # execution_date_str을 datetime 객체로 변환
    execution_date = datetime.strptime(execution_date_str, '%Y-%m-%dT%H:%M:%S%z')
    bucket = 'pdc3project-stage-layer-bucket'
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_GCS')
    dst_path = f'source/source_parkinglot/GMP_parking_data/2024/06/{formatted_day}/GMP_parking_data_{formatted_timestamp}.parquet'

    # Parquet 파일을 GCS에 업로드
    gcs_hook.upload(bucket_name=bucket, object_name=dst_path, filename=parquet_path)


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack.on_failure_callback,
}

dag = DAG(
    'GMP_csv_to_parquet',
    default_args=default_args,
    description='CSV 데이터를 Parquet으로 변환하여 GCS에 저장하는 DAG',
    schedule_interval='*/5 * * * *',
    max_active_runs=1,
    catchup=False,
)

set_kst_task = PythonOperator(
    task_id='set_kst_execution_date',
    python_callable=set_kst_execution_date,
    provide_context=True,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_csv_from_gcs',
    python_callable=extract_csv_from_gcs,
    op_kwargs={'execution_date_str': '{{ ts }}'},  # ts는 실행 날짜와 시간을 문자열로 반환합니다.
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_csv_to_parquet',
    python_callable=transform_csv_to_parquet,
    op_kwargs={'execution_date_str': '{{ ts }}'},
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_gcs',
    python_callable=upload_to_gcs,
    op_kwargs={'execution_date_str': '{{ ts }}'},
    dag=dag,
)

set_kst_task >> extract_task >> transform_task >> upload_task
