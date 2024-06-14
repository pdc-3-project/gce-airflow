from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'FS_raw_to_adhoc',
    default_args=default_args,
    description='ETL from raw_data to adhoc in BigQuery',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    table_names = ['flight_data_GIMPO', 'flight_data_INCHEON', 'flight_data_JEJU']

    for table_name in table_names:
        task = BigQueryInsertJobOperator(
            task_id=f'extract_transform_load_{table_name}',
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE TABLE pdc3project.adhoc.{table_name} AS
                        SELECT
                          AIRLINE_KOREAN,
                          AIRPORT,
                          ARRIVED_KOR,
                          BOARDING_KOR,
                          FLIGHT_DATE,
                          IO,
                          LINE,
                          RMK_KOR,
                          STD,
                          ETD,
                          UFID,
                          CASE 
                            WHEN STD = 0 OR ETD = 0 THEN 0
                            ELSE ROUND(
                                ((FLOOR(STD / 100) * 60 + MOD(STD, 100)) - (FLOOR(ETD / 100) * 60 + MOD(ETD, 100))),
                                2
                            )
                          END AS DELAY_TIME
                        FROM
                          pdc3project.raw_data.{table_name};
                    """,
                    "useLegacySql": False,
                }
            },
            location='asia-northeast3',  # BigQuery 데이터셋이 위치한 실제 지리적 위치
            gcp_conn_id='google_cloud_bigquery',  # Airflow에서 설정한 GCP 연결 ID
            dag=dag,
        )
