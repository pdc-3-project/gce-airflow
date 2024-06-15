from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago

import pandas as pd
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
}

# 새로운 데이터를 Pandas DataFrame으로 로드
def load_new_data(**kwargs):
    hook = BigQueryHook(gcp_conn_id='google_cloud_bigquery', use_legacy_sql=False)
    sql = """
    WITH union_all_airports AS (
        SELECT *
        FROM (
            SELECT *, '서울/김포' AS target_airport FROM `pdc3project.raw_data.flight_data_GIMPO`
            UNION ALL
            SELECT *, '인천' AS target_airport FROM `pdc3project.raw_data.flight_data_INCHEON`
            UNION ALL
            SELECT *, '제주' AS target_airport FROM `pdc3project.raw_data.flight_data_JEJU`
        )
    ),
    ARR_CTE as (
        SELECT
            CASE 
            WHEN fs.ARRIVED_KOR = ac.cityKor
                THEN ac.cityCode 
            END as ARRIVED_CODE,
            * 
        FROM union_all_airports fs
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
        ETD,
        target_airport
    FROM BOARD_COOR_CTE
    """
    df = hook.get_pandas_df(sql)
    kwargs['ti'].xcom_push(key='new_data', value=df.to_dict(orient='list'))

# 기존 데이터를 Pandas DataFrame으로 로드
def load_existing_data(**kwargs):
    hook = BigQueryHook(gcp_conn_id='google_cloud_bigquery', use_legacy_sql=False)
    sql = "SELECT * FROM `pdc3project.analytics.flight_map`"
    df = hook.get_pandas_df(sql)
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

    gcs_object_name = f'source/flight_route_data/{ execution_date.strftime("%Y/%m/%d") }/flight_route_data_{ execution_date.strftime("%Y%m%d") }.parquet'

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
        table = pa.Table.from_pandas(combined_df)
        pq.write_table(table, temp_file.name, coerce_timestamps='us', use_deprecated_int96_timestamps=True)
        temp_file.flush()
        gcs_hook = GCSHook(google_cloud_storage_conn_id='google_cloud_GCS')
        gcs_hook.upload(
            bucket_name='pdc3project-analytics-layer-bucket',
            object_name=gcs_object_name,
            filename=temp_file.name
        )

    bq_source_uris = f'gs://pdc3project-analytics-layer-bucket/{ gcs_object_name }'

    hook = BigQueryHook(gcp_conn_id='google_cloud_bigquery', use_legacy_sql=False)
    hook.run_load(
        destination_project_dataset_table='pdc3project.analytics.flight_map',
        source_uris=[bq_source_uris],
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
    )

# DAG 정의
with DAG(
    'update_flight_map_table',
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

    # 작업의 순서 정의
    [load_new_data_task, load_existing_data_task] >> merge_and_remove_duplicates_task >> update_final_table_task