from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'create_flight_map_table',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    create_flight_map_table = BigQueryInsertJobOperator(
        task_id='create_flight_map_table',
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE `pdc3project.analytics.flight_map` AS
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
                ARRIVED_KOR,
                BOARDING_KOR,
                origin_lat,
                origin_lon,
                destination_lat,
                destination_lon,
                ETD,
                target_airport
                FROM
                BOARD_COOR_CTE
                """,
                "useLegacySql": False,
            }
        },
        location='asia-northeast3',
    )

    create_flight_map_table
