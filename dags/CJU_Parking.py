from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from airflow.models import Variable
from plugins import slack
import json


def fetch_xml_data(airport_code):
    url = 'http://openapi.airport.co.kr/service/rest/AirportParkingCongestion/airportParkingCongestionRT'
    parking_api_key = Variable.get('parking_api_key')
    api_key_decode = requests.utils.unquote(parking_api_key)
    params = {
        'schAirportCode': airport_code,
        'serviceKey': api_key_decode,
        'numOfRows': 10,
        'pageNo': 1
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.text

def calculate_date(sysGetdate, sysGettime):
    datetm = f"{sysGetdate[:4]}{sysGetdate[5:7]}{sysGetdate[8:]}{sysGettime[:2]}{sysGettime[3:5]}{sysGettime[6:]}"
    return datetm

# Function to fetch parking data for a specific airport code
def parse_xml_data(xml_data):
    root = ET.fromstring(xml_data)
    items = []
    for item in root.find('body').find('items').findall('item'):
        sysGetdate = item.find('sysGetdate').text
        sysGettime = item.find('sysGettime').text
        datetm = calculate_date(sysGetdate, sysGettime)

        items.append({
            'airportKor': item.find('airportKor').text,
            'parkingAirportCodeName': item.find('parkingAirportCodeName').text,
            'parkingCongestion': item.find('parkingCongestion').text,
            'parkingCongestionDegree': item.find('parkingCongestionDegree').text,
            'parkingOccupiedSpace': item.find('parkingOccupiedSpace').text,
            'parkingTotalSpace': item.find('parkingTotalSpace').text,
            'datetm': datetm
        })
    return items


# Main function to fetch and upload data for both GMP and CJU
def update_parking_data():
    data_cju = fetch_xml_data('CJU')
    parsed_data_cju = parse_xml_data(data_cju)
    df = pd.DataFrame(parsed_data_cju)
    df.to_csv('/tmp/CJU_parking_data.csv', index=False, encoding='utf-8-sig')
    print("Domestic data fetched and saved to '/tmp/CJU_parking_data.csv'")


# Default settings for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack.on_failure_callback,
}

# Define the DAG
dag = DAG(
    'fetch_and_CJU_parking_data',
    default_args=default_args,
    description='Fetch parking data from OpenAPI for GMP and CJU and store in GCS every 5 minutes',
    schedule_interval='*/5 * * * *',
    catchup=False,
)

# Define the task
fetch_and_upload_task = PythonOperator(
    task_id='fetch_and_upload_parking_data',
    python_callable=update_parking_data,
    dag=dag,
)

upload_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id='fetch_and_upload_CJU_parking_data', # 개발자의 task 정의에 따라 임의로 입력
    src='/tmp/CJU_parking_data.csv', # API에서 받아온 데이터를 로컬에 csv로 먼저 저장한 상황을 가정하며 해당 csv파일 위치 입력
    dst='source/source_parkinglot/CJU_parking_data/2024/06/{{ execution_date.strftime("%d") }}/CJU_parking_data_{{ execution_date.strftime("%Y%m%d") }}.csv',
    bucket='pdc3project-landing-zone-bucket',
    gcp_conn_id='google_cloud_GCS',
    dag=dag
)

fetch_and_upload_task>>upload_to_gcs_task