from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from google.cloud import storage
from plugins import slack
import json


# Function to fetch XML data from the API
def fetch_xml_data():
    url = "https://apis.data.go.kr/B551177/StatusOfParking/getTrackingParking"
    api_key = "l6Tnmj2Ivn4jziqbVMJUqhfHuyJpqgRmSTHWyFIp008ClGqenqCjvltiU7Dm69VXRYdL78I0p34RkEeUE9wqeA%3D%3D"
    api_key_decode = requests.utils.unquote(api_key)
    params = {
        'serviceKey': api_key_decode,
    }
    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise an error if the request failed
    return response.text


# Function to calculate parking congestion degree and status
def calculate_congestion(parking, parkingarea):
    degree = (parking / parkingarea) * 100
    if degree <= 90:
        congestion = '원활'
    elif 90 < degree <= 95:
        congestion = '혼잡'
    else:
        congestion = '만차'
    return degree, congestion


# Function to parse the XML data and extract the required fields
def parse_xml_data(xml_data):
    root = ET.fromstring(xml_data)
    items = []
    for item in root.findall('.//item'):
        parking = int(item.find('parking').text)
        parkingarea = int(item.find('parkingarea').text)
        degree, congestion = calculate_congestion(parking, parkingarea)

        parsed_item = {
            'airportKor': '인천국제공항',
            'parkingAirportCodeName': item.find('floor').text,
            'parkingCongestion': congestion,
            'parkingCongestionDegree': f"{degree:.2f}%",
            'parkingOccupiedSpace': parking,
            'parkingTotalSpace': parkingarea,
            'datetm': item.find('datetm').text[:14]
        }
        items.append(parsed_item)
    return items


# Function to save the data to Google Cloud Storage
def update_parking_data():
    data_int = fetch_xml_data()
    parsed_data_int = parse_xml_data(data_int)
    df = pd.DataFrame(parsed_data_int)
    df.to_csv('/tmp/INT_parking_data.csv', index=False)
    print("Domestic data fetched and saved to '/tmp/INT_parking_data.csv'")


# Define the default_args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack.on_failure_callback,
}

# Define the DAG
dag = DAG(
        'update_INT_parking_data',
        default_args=default_args,
        description='A DAG to update parking data every 5 minutes and save it to GCS',
        schedule_interval='*/5 * * * *',
        catchup=False,
)

fetch_and_upload_task = PythonOperator(
    task_id='fetch_and_upload_INT_parking_data',
    python_callable=update_parking_data,
    dag=dag,
)


upload_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id='fetch_and_upload_INT_parking_data', # 개발자의 task 정의에 따라 임의로 입력
    src='/tmp/GMP_parking_data.csv', # API에서 받아온 데이터를 로컬에 csv로 먼저 저장한 상황을 가정하며 해당 csv파일 위치 입력
    dst='source/source_parkinglot/INT_parking_data/2024/06/{{ execution_date.strftime("%d") }}/INT_parking_data_{{ execution_date.strftime("%Y%m%d") }}.csv',
    bucket='pdc3project-landing-zone-bucket',
    gcp_conn_id='google_cloud_GCS',
    dag=dag
)

fetch_and_upload_task>>upload_to_gcs_task

