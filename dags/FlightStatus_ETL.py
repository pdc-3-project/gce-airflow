# 실시간 항공 정보
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import pandas as pd
import requests
import xml.etree.ElementTree as ET
import logging
from plugins import slack
from airflow.models import Variable

# Airflow Variables
service_key = Variable.get('service_key')

def send_request(url, params):
    response = requests.get(url, params=params)
    return response.text

def xml_to_dict(element):
    result = {}
    for child in element:
        if len(child) > 0:
            result[child.tag] = xml_to_dict(child)
        else:
            result[child.tag] = child.text
    return result

def add_data_to_list(response_text, all_data):
    root = ET.fromstring(response_text)
    body = root.find('./body')
    if body is not None:
        items = body.find('./items')
        if items is not None:
            for item in items.findall('./item'):
                data_dict = xml_to_dict(item)
                all_data.append(data_dict)

def extract_flight_status_data(**kwargs):
    base_url = "http://openapi.airport.co.kr/service/rest"
    url = "/FlightStatusList/getFlightStatusList"

    params = {
        'serviceKey': service_key,
        'schDate': execution_date.strftime('%Y%m%d'),  # DAG 실행 날짜 사용
    }

    all_data = []

    response_text = send_request(base_url + url, params)
    add_data_to_list(response_text, all_data)

    root = ET.fromstring(response_text)
    total_count = int(root.findtext('./body/totalCount'))
    num_of_rows = int(root.findtext('./body/numOfRows'))
    total_pages = (total_count + num_of_rows - 1) // num_of_rows

    for page in range(2, total_pages + 1):
        params['pageNo'] = str(page)
        response_text = send_request(base_url + url, params)
        add_data_to_list(response_text, all_data)

    return all_data


def transform_flight_status_data(**kwargs):
    ti = kwargs['ti']
    all_data = ti.xcom_pull(task_ids='extract_flight_status_data')

    selected_columns = ['airlineKorean', 'airFln', 'airport', 'arrivedKor', 'boardingKor', 'city', 'etd', 'flightDate', 'io', 'line', 'rmkKor', 'std']
    filtered_data = [
        {key: item[key] for key in selected_columns if key in item}
        for item in all_data
    ]

    df = pd.DataFrame(filtered_data)
    df_filtered = df[(df['arrivedKor'].isin(['김포', '인천', '제주'])) | (df['boardingKor'].isin(['김포', '인천', '제주']))]

    df_filtered.to_csv('/tmp/flight_data.csv', index=False)
    logging.info("Flight data fetched and saved to /tmp/flight_data.csv")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback' : slack.on_failure_callback
}

dag = DAG(
    dag_id = 'flight_data_fetcher',
    default_args=default_args,
    catchup = False,
    schedule = '0 0 * * *',
    
)

extract_data_task = PythonOperator(
    task_id='extract_flight_status_data',
    python_callable=extract_flight_status_data,
    provide_context=True,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_flight_status_data',
    python_callable=transform_flight_status_data,
    provide_context=True,
    dag=dag,
)

upload_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id='upload_to_gcs',
    src='/tmp/flight_data.csv',
    dst='source/flight_data/{{ execution_date.strftime("%Y/%m/%d") }}/flight_data_{{ execution_date.strftime("%Y%m%d") }}.csv',
    bucket='pdc3project-landing-layer-bucket',
    gcp_conn_id='google_cloud_GCS',
    dag=dag,
)

extract_data_task >> transform_data_task >> upload_to_gcs_task
