from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago

from plugins import slack

import pandas as pd
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'on_failure_callback': slack.on_failure_callback,
}

# 새로운 데이터를 Pandas DataFrame으로 로드
def load_new_data(**kwargs):
    hook = BigQueryHook(gcp_conn_id='google_cloud_bigquery', use_legacy_sql=False, location='asia-northeast3')
    sql = """
    WITH ARR_CTE as (
        SELECT
            CASE 
            WHEN fs.ARRIVED_KOR = ac.cityKor
                THEN ac.cityCode 
            END as ARRIVED_CODE,
            * 
        FROM `pdc3project.raw_data.flight_data` fs
        LEFT JOIN `pdc3project.raw_data.airport_codes` ac
        ON fs.ARRIVED_KOR = ac.cityKor
    ),
    BOARD_CTE as (
        SELECT
            CASE
            WHEN acte.BOARDING_KOR = ac2.cityKor
                THEN ac2.cityCode
            END as BOARDING_CODE,
            * 
        FROM ARR_CTE acte
        LEFT JOIN `pdc3project.raw_data.airport_codes` ac2
        ON acte.BOARDING_KOR = ac2.cityKor
    ),
    ARR_COOR_CTE as (
        SELECT
            CASE 
            WHEN bcte.ARRIVED_CODE = acoor.iata_code
                THEN acoor.latitude_deg
            END as origin_lat,
            CASE 
            WHEN bcte.ARRIVED_CODE = acoor.iata_code
                THEN acoor.longitude_deg
            END as origin_lon,
            *
        FROM BOARD_CTE bcte
        LEFT JOIN `pdc3project.raw_data.airport_coordinates` acoor
        ON bcte.ARRIVED_CODE = acoor.iata_code
    ),
    BOARD_COOR_CTE as (
        SELECT
            CASE
            WHEN act.BOARDING_CODE = acoor2.iata_code
                THEN acoor2.latitude_deg
            END as destination_lat,
            CASE
            WHEN act.BOARDING_CODE = acoor2.iata_code
                THEN acoor2.longitude_deg
            END as destination_lon,
            *
        FROM ARR_COOR_CTE act
        LEFT JOIN `pdc3project.raw_data.airport_coordinates` acoor2
        ON act.BOARDING_CODE = acoor2.iata_code
    )
    SELECT
        UFID,
        ARRIVED_KOR,
        BOARDING_KOR,
        origin_lat,
        origin_lon,
        destination_lat,
        destination_lon,
        PARSE_TIMESTAMP('%Y%m%d%H%M', CONCAT(SUBSTR(UFID, 1, 8), LPAD(CAST(ETD AS STRING), 4, '0'))) as ETD,
        FLIGHT_DATE,
        target_airport
    FROM BOARD_COOR_CTE
    """
    df = hook.get_pandas_df(sql, dialect='standard')
    df['ETD'] = df['ETD'].astype(str)
    kwargs['ti'].xcom_push(key='new_data', value=df.to_dict(orient='list'))

# 기존 데이터를 Pandas DataFrame으로 로드
def load_existing_data(**kwargs):
    hook = BigQueryHook(gcp_conn_id='google_cloud_bigquery', location='asia-northeast3')

    # 테이블 존재 여부 확인 쿼리
    check_table_exists_sql = """
        SELECT 
            COUNT(1) AS table_exists
        FROM `pdc3project.analytics.INFORMATION_SCHEMA.TABLES`
        WHERE table_name = 'flight_map'
    """
    
    # 테이블 존재 여부 확인
    table_exists_df = hook.get_pandas_df(check_table_exists_sql, dialect='standard')
    table_exists = table_exists_df['table_exists'].iloc[0] > 0

    if not table_exists:
        # 테이블이 존재하지 않으면 빈 데이터프레임 반환
        df = pd.DataFrame(columns=[
            'UFID','ARRIVED_KOR', 'BOARDING_KOR', 'origin_lat', 'origin_lon', 
            'destination_lat', 'destination_lon', 'ETD', 'FLIGHT_DATE','target_airport'
        ])
    else:
        # 테이블이 존재하면 데이터를 로드
        sql = "SELECT * FROM `pdc3project.analytics.flight_map`"
        df = hook.get_pandas_df(sql, dialect='standard')

    df['ETD'] = df['ETD'].astype(str)
    kwargs['ti'].xcom_push(key='existing_data', value=df.to_dict(orient='list'))

# 데이터를 병합하고 중복 제거
def merge_and_remove_duplicates(**kwargs):
    ti = kwargs['ti']
    new_data = ti.xcom_pull(key='new_data')
    existing_data = ti.xcom_pull(key='existing_data')
    
    df_new = pd.DataFrame(new_data)
    df_existing = pd.DataFrame(existing_data)
    
    combined_df = pd.concat([df_new, df_existing]).drop_duplicates(subset=['UFID', 'target_airport'])
    combined_dict = combined_df.to_dict(orient='list')
    ti.xcom_push(key='combined_data', value=combined_dict)

# 최종 테이블 업데이트
def update_final_table(**kwargs):
    ti = kwargs['ti']
    execution_date = kwargs['execution_date']
    combined_data = ti.xcom_pull(key='combined_data')
    combined_df = pd.DataFrame(combined_data)
    combined_df['ETD'] = combined_df['ETD'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z') if pd.notnull(x) else x)


    gcs_object_name = f'source/flight_route_data/{ execution_date.strftime("%Y/%m/%d") }/flight_route_data_{ execution_date.strftime("%Y%m%d") }.parquet'

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
        table = pa.Table.from_pandas(combined_df)
        pq.write_table(table, temp_file.name, coerce_timestamps='us', use_deprecated_int96_timestamps=True)
        temp_file.flush()
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_GCS')
        gcs_hook.upload(
            bucket_name='pdc3project-analytics-layer-bucket',
            object_name=gcs_object_name,
            filename=temp_file.name
        )

    bq_source_uris = f'gs://pdc3project-analytics-layer-bucket/{ gcs_object_name }'

    hook = BigQueryHook(gcp_conn_id='google_cloud_bigquery', location='asia-northeast3')
    hook.run_load(
        destination_project_dataset_table='pdc3project.analytics.flight_map',
        source_uris=[bq_source_uris],
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True
    )


with DAG(
    'update_flight_map_table',
    description= "an aircraft flight route data table fetch dag using flight_data related tables and static data from bigquery's raw_data dataset. Flight_data tables are updated incrementally every day according to their update cycle. Updated in both GCS and bigquery analytics dataset.",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # 새로운 데이터 로드 작업
    load_new_data_task = PythonOperator(
        task_id='load_new_data',
        python_callable=load_new_data,
    )

    # 기존 데이터 로드 작업
    load_existing_data_task = PythonOperator(
        task_id='load_existing_data',
        python_callable=load_existing_data,
    )

    # 데이터 병합 및 중복 제거 작업
    merge_and_remove_duplicates_task = PythonOperator(
        task_id='merge_and_remove_duplicates',
        python_callable=merge_and_remove_duplicates,
    )

    # 최종 테이블 업데이트 작업
    update_final_table_task = PythonOperator(
        task_id='update_final_table',
        python_callable=update_final_table,
    )

    [load_new_data_task, load_existing_data_task] >> merge_and_remove_duplicates_task >> update_final_table_task