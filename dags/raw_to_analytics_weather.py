from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import timedelta
from datetime import datetime
from plugins import slack


with DAG(
    dag_id='raw_to_analytics_weather',
    description='ETL from raw_data to analytics in BigQuery',
    schedule_interval='@daily',
    start_date=datetime(2024, 6, 1, 00, 30),
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
        'on_failure_callback': slack.on_failure_callback,
    }
) as dag:

    query = """
        DROP TABLE IF EXISTS analytics.up_30m_with_weather;
        CREATE TABLE analytics.up_30m_with_weather AS 
        SELECT DISTINCT(SUBSTRING(wi.TM, 5, 8)) AS STD, fda.BOARDING_KOR AS DEPARTURE_AP, (fda.ETD-fda.STD) AS DELAY_TIME, wi.WS10/10 AS WS10, wi.HM, wi.PA/10 AS PA, wi.TA/10 AS TA, wi.RN/10 AS RN 
        FROM (
        SELECT *, '서울/김포' AS target_airport FROM `pdc3project.raw_data.flight_data_GIMPO`
            UNION ALL
        SELECT *, '인천' AS target_airport FROM `pdc3project.raw_data.flight_data_INCHEON`
            UNION ALL
        SELECT *, '제주' AS target_airport FROM `pdc3project.raw_data.flight_data_JEJU`
        ) AS fda
        JOIN (
            SELECT w.TM, w.WS10, w.HM, w.PA, w.TA, w.RN, wac.AIRPORT_NAME, wac.AIRPORT_CODE
            FROM `pdc3project.raw_data.weather_infor` AS w
            JOIN `pdc3project.raw_data.weather_airport_conn` AS wac
            ON w.S = wac.S
        ) wi
        ON LEFT(wi.AIRPORT_NAME, 2) = RIGHT(fda.BOARDING_KOR, 2)
        AND CAST(SUBSTRING(wi.TM, 1, 4) AS INT64) = EXTRACT(YEAR FROM fda.FLIGHT_DATE)
        AND CAST(SUBSTRING(wi.TM, 5, 2) AS INT64) = EXTRACT(MONTH FROM fda.FLIGHT_DATE)
        AND CAST(SUBSTRING(wi.TM, 7, 2) AS INT64) = EXTRACT(DAY FROM fda.FLIGHT_DATE)
        AND CAST(SUBSTRING(wi.TM, 9, 4) AS INT64) = fda.STD
        WHERE (fda.ETD-fda.STD) >= 30
        ORDER BY fda.BOARDING_KOR, DELAY_TIME DESC, STD;
    """

    raw_to_analytics = BigQueryInsertJobOperator(
        task_id="raw_to_analytics",
        configuration={
            "query": {
                "query": query,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_bigquery',
        dag=dag,
    )

