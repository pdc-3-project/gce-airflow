from datetime import datetime, timedelta
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from plugins import slack  
from airflow.models import Variable

# Airflow Variables
service_key = Variable.get('airport_api_key')

def fetch_data_from_api(url, params):
    response = requests.get(url, params=params)
    return response.json()

def extract_flight_data(**kwargs):
    base_url = "https://api.odcloud.kr/api"
    url = "/FlightStatusListDTL/v1/getFlightStatusListDetail"
    execution_date = kwargs['execution_date']

    all_data = []
    page = 1
    per_page = 50

    while True:
        params = {
            'page': page,
            'perPage': per_page,
            'returnType': 'JSON',
            'cond[FLIGHT_DATE::EQ]': execution_date.strftime('%Y%m%d'),
            'serviceKey': service_key,
        }

        response_json = fetch_data_from_api(base_url + url, params=params)

        if 'data' in response_json:
            data = response_json['data']
            if not data:
                break
            all_data.extend(data)
            page += 1
        else:
            print("Error in API response:", response_json)
            break

    return all_data

def transform_flight_data(**kwargs):
    ti = kwargs['ti']
    all_data = ti.xcom_pull(task_ids='extract_flight_data')

    filtered_data = [
        {key: flight.get(key, '') for key in ['AIRLINE_KOREAN', 'AIRPORT', 'ARRIVED_KOR', 'BOARDING_KOR', 'FLIGHT_DATE', 'IO', 'LINE', 'RMK_KOR', 'STD', 'UFID']}
        for flight in all_data 
        if flight.get('BOARDING_KOR', '') in ['김포', '인천', '제주'] or flight.get('ARRIVED_KOR', '') in ['김포', '인천', '제주']
    ]

    df = pd.DataFrame(filtered_data)
    csv_file_path = '/tmp/FlightStatus.csv'
    df.to_csv(csv_file_path, index=False)

    return csv_file_path

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': slack.on_failure_callback  
}

dag = DAG(
    dag_id='flight_data_fetcher',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 0 * * *', 
)

extract_flight_data_task = PythonOperator(
    task_id='extract_flight_data',
    python_callable=extract_flight_data,
    provide_context=True,
    dag=dag,
)

transform_flight_data_task = PythonOperator(
    task_id='transform_flight_data',
    python_callable=transform_flight_data,
    provide_context=True,
    dag=dag,
)

upload_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id='upload_to_gcs',
    src='/tmp/FlightStatus.csv',
    dst='source/flight_data/{{ execution_date.strftime("%Y/%m/%d") }}/flight_data_{{ execution_date.strftime("%Y%m%d") }}.csv',
    bucket='pdc3project-landing-zone-bucket',
    gcp_conn_id='google_cloud_GCS',
    dag=dag,
)

extract_flight_data_task >> transform_flight_data_task >> upload_to_gcs_task
