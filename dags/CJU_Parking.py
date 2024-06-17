from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from airflow.models import Variable
from plugins import slack
import pytz


# Function to fetch XML data from the API
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


# Function to calculate parking congestion degree and status

def calculate_date(sysGetdate, sysGettime):
    datetm = f"{sysGetdate[:4]}{sysGetdate[5:7]}{sysGetdate[8:]}{sysGettime[:2]}{sysGettime[3:5]}{sysGettime[6:]}"
    return datetm

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
            'parkingCongestionDegree': int(item.find('parkingCongestionDegree').text[:2]),
            'parkingOccupiedSpace': item.find('parkingOccupiedSpace').text,
            'parkingTotalSpace': item.find('parkingTotalSpace').text,
            'datetm': datetm
        })
    return items


# Function to upload data to Google Cloud Storage
def update_parking_data():
    data_gmp = fetch_xml_data('CJU')
    parsed_data_gmp = parse_xml_data(data_gmp)
    df = pd.DataFrame(parsed_data_gmp)
    df.to_csv('/tmp/GMP_parking_data.csv', index=False, encoding='utf-8-sig')
    print("Domestic data fetched and saved to '/tmp/CJU_parking_data.csv'")


# Convert UTC to KST
def convert_to_kst(execution_date):
    # UTC에서 한국 시간으로 변환
    execution_date_utc = execution_date.replace(tzinfo=pytz.UTC)
    execution_date_kst = execution_date_utc.astimezone(pytz.timezone('Asia/Seoul'))
    return execution_date_kst.strftime('%Y-%m-%d %H:%M:%S')

def set_kst_execution_date(**kwargs):
    # Airflow의 execution_date를 가져와서 한국 시간대로 변환 후 XCom에 저장
    execution_date = kwargs['execution_date']
    execution_date_kst = convert_to_kst(execution_date)
    ti = kwargs['ti']
    ti.xcom_push(key='execution_date_kst', value=execution_date_kst)

# Function to retrieve the KST execution date from XCom and format the destination path
def get_kst_execution_date_path(**kwargs):
    ti = kwargs['ti']
    execution_date_kst = ti.xcom_pull(task_ids='set_kst_execution_date', key='execution_date_kst')
    # Convert the pulled execution_date_kst string back to a datetime object
    execution_date_kst_dt = datetime.strptime(execution_date_kst, '%Y-%m-%d %H:%M:%S')
    formatted_day = execution_date_kst_dt.strftime("%d")
    formatted_timestamp = execution_date_kst_dt.strftime("%Y%m%d%H%M")
    return f'source/source_parkinglot/CJU_parking_data/2024/06/{formatted_day}/CJU_parking_data_{formatted_timestamp}.csv'

def upload_to_gcs_callable(**kwargs):
    dst_path = get_kst_execution_date_path(**kwargs)
    # Use the path in LocalFilesystemToGCSOperator
    LocalFilesystemToGCSOperator(
        task_id='upload_CJU_parking_data',
        src='/tmp/CJU_parking_data.csv',
        dst=dst_path,
        bucket='pdc3project-landing-zone-bucket',
        gcp_conn_id='google_cloud_GCS',
        dag=kwargs['dag'],
    ).execute(kwargs)


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
    'update_CJU_parking_data',
    default_args=default_args,
    description='A DAG to update parking data every 5 minutes and save it to GCS',
    schedule_interval='*/5 * * * *',
    catchup=False,
)

fetch_and_upload_task = PythonOperator(
    task_id='fetch_and_upload_CJU_parking_data',
    python_callable=update_parking_data,
    dag=dag,
)

set_kst_task = PythonOperator(
    task_id='set_kst_execution_date',
    python_callable=set_kst_execution_date,
    provide_context=True,
    dag=dag,
)

upload_to_gcs_task = PythonOperator(
    task_id='upload_CJU_airplane_parking_data',
    python_callable=upload_to_gcs_callable,
    provide_context=True,
    dag=dag,
)

fetch_and_upload_task >> set_kst_task >> upload_to_gcs_task
