from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'FS_raw_to_analytics',
    default_args=default_args,
    description='ETL from raw_data to analytics in BigQuery',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:
    task = BigQueryInsertJobOperator(
        task_id='raw_to_analytics',
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE pdc3project.analytics.flight_data AS
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
                      target_airport,
                      CASE 
                        WHEN STD = 0 OR ETD = 0 THEN 0
                        ELSE ROUND(
                            ((FLOOR(STD / 100) * 60 + MOD(STD, 100)) - (FLOOR(ETD / 100) * 60 + MOD(ETD, 100))),2)
                      END AS DELAY_TIME
                    FROM
                      pdc3project.raw_data.flight_data;
                """,
                "useLegacySql": False,
            }
        },
        location='asia-northeast3',
        gcp_conn_id='google_cloud_bigquery',
        dag=dag,
    )
