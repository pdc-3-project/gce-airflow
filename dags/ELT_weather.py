from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.hooks.gcs_hook import GCSHook
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta
from plugins import slack

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


@task
def download_gcs_bucket(gcp_conn_id, bucket_name, folder_path, local_dir):

    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    
    # 로컬 디렉토리 생성
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    
    # GCS 버킷 폴더의 객체(파일) 목록 가져오기
    objects = gcs_hook.list(bucket_name=bucket_name, prefix=folder_path)
    
    # 각 객체(파일) 다운로드
    for obj in objects:
        local_path = os.path.join(local_dir, os.path.basename(obj))
        gcs_hook.download(bucket_name=bucket_name, object_name=obj, filename=local_path)


@task
def sum_csv_transform_csv_to_parquet():
    csv_files = [f for f in os.listdir('tmp/GCS') if f.endswith('.csv')]

    records = []
    for csv_file in csv_files:
        file_path = os.path.join('tmp/GCS', csv_file)
        line = (open(file_path, 'r').read()).strip().split("\n") 
        for l in line[1:]:
            (NUMBERING1, NUMBERING2, S, TM, L_VIS, R_VIS, L_RVR, R_RVR, CH_MIN, TA, TD, HM, PS, PA, RN, B1, B2, WD02, WD02_MAX, WD02_MIN, WS02, WS02_MAX, WS02_MIN, WD10, WD10_MAX, WD10_MIN, WS10, WS10_MAX, WS10_MIN) = l.split(",")
            TM = datetime.strptime(TM, '%Y%m%d%H%M')
            records.append([int(S), TM, float(L_VIS), float(L_RVR), float(CH_MIN), float(TA), float(HM), float(PA), float(RN), float(WS02), float(WS02_MAX), float(WS02_MIN), float(WS10), float(WS10_MAX), float(WS10_MIN)])
            print(TM)
    df = pd.DataFrame(records, columns=['S', 'TM', 'L_VIS', 'L_RVR', 'CH_MIN', 'TA', 'HM', 'PA', 'RN', 'WS02', 'WS02_MAX', 'WS02_MIN', 'WS10', 'WS10_MAX', 'WS10_MIN'])
    table = pa.Table.from_pandas(df)
    pq.write_table(table, 'tmp/airport_weather_infor.parquet')


@task
def delete_csv_files(local_dir):
    csv_files = [f for f in os.listdir(local_dir) if f.endswith('.csv')]

    for csv_file in csv_files:
        file_path = os.path.join(local_dir, csv_file)
        os.remove(file_path)


with DAG(
    dag_id='Weather_ELT',
    start_date=datetime(2024, 6, 1, 00, 10),
    schedule=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:


    upload_gcs_stage = LocalFilesystemToGCSOperator(
    task_id='upload_to_gcs_stage', 
    src='tmp/airport_weather_infor.parquet',
    dst='source/weather/{{ execution_date.in_timezone("Asia/Seoul").strftime("%Y/%m/%d") }}/weather_infor_{{ execution_date.in_timezone("Asia/Seoul").strftime("%Y%m%d") }}.parquet', 
    bucket='pdc3project-stage-layer-bucket',
    gcp_conn_id='google_cloud_GCS',
    dag=dag 
    )


    upload_to_bigquery = GCSToBigQueryOperator(
    task_id='upload_to_bq',
    bucket='pdc3project-stage-layer-bucket',
    source_objects=['source/weather/{{ execution_date.in_timezone("Asia/Seoul").strftime("%Y/%m/%d") }}/weather_infor_{{ execution_date.in_timezone("Asia/Seoul").strftime("%Y%m%d") }}.parquet'],
    destination_project_dataset_table='pdc3project.raw_data.weather_infor',
    source_format='PARQUET',
    autodetect=True,
    write_disposition='WRITE_APPEND',
    gcp_conn_id='google_cloud_bigquery',
    dag=dag
    )
    
    gcp_conn_id = 'google_cloud_GCS'
    bucket_name = 'pdc3project-landing-zone-bucket'
    folder_path = 'source/weather/{{ execution_date.in_timezone("Asia/Seoul").strftime("%Y/%m/%d") }}'
    local_dir = 'tmp/GCS'


    download_gcs_bucket(gcp_conn_id, bucket_name, folder_path, local_dir) >> sum_csv_transform_csv_to_parquet() >> upload_gcs_stage >> delete_csv_files(local_dir) >> upload_to_bigquery
