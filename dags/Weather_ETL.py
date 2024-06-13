from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta
from plugins import slack

import requests
import io
import pandas as pd


@task
def extract(api_key, date_now):
    # 맨 앞, 뒤에 불필요한 행 + 컬럼 제거
    def delete_first_end(text):
        lines = text.strip().split('\n')
        return str('\n'.join(lines[3:-1]))

    
    domain = "https://apihub.kma.go.kr/api/typ01/url/amos.php?"
    tmp_dt = datetime.strptime(date_now.replace("T"," ")[:19], '%Y-%m-%d %H:%M:%S')
    dt_exec = tmp_dt + timedelta(hours=9) - timedelta(minutes=5)  # 110인 김포공항의 업데이트가 일부 느려서 5분 딜레이
    tm = dt_exec.strftime("%Y%m%d%H%M")  # tm = YYYYmmddHHMM format으로 맞추기
    dtm = "59"  # tm부터 tm 기준 dtm분 전 데이터까지 -> 총 60개의 데이터
    stn1 = "110"  # 김포공항 지점코드
    stn2 = "113"  # 인천공항 지점코드
    stn3 = "182"  # 제주공항 지점코드
    help_type = "0"  # 도움말 유무 
    weather_api_key = api_key

    url_kimpo = requests.get(f'{domain}tm={tm}&dtm={dtm}&stn={stn1}&help={help_type}&authKey={weather_api_key}')
    url_incheon = requests.get(f'{domain}tm={tm}&dtm={dtm}&stn={stn2}&help={help_type}&authKey={weather_api_key}')
    url_jeju = requests.get(f'{domain}tm={tm}&dtm={dtm}&stn={stn3}&help={help_type}&authKey={weather_api_key}')

    url = delete_first_end(url_kimpo.text) + '\n' + delete_first_end(url_incheon.text)  + '\n' + delete_first_end(url_jeju.text)

    data = pd.read_csv(io.StringIO(url), sep='\s+')
    data.to_csv('airport_weather_infor.csv', encoding="utf-8-sig")

    return open('airport_weather_infor.csv', 'r').read()


@task
def transform(text):
    lines = text.strip().split("\n") 
    records = []
    for l in lines:
        print(l)
        (NUMBERING, S, TM, L_VIS, R_VIS, L_RVR, R_RVR, CH_MIN, TA, TD, HM, PS, PA, RN, B1, B2, WD02, WD02_MAX, WD02_MIN, WS02, WS02_MAX, WS02_MIN, WD10, WD10_MAX, WD10_MIN, WS10, WS10_MAX, WS10_MIN) = l.split(",")
        if NUMBERING == '\ufeff':
            NUMBERING = -1
        records.append([NUMBERING, S, TM, L_VIS, R_VIS, L_RVR, R_RVR, CH_MIN, TA, TD, HM, PS, PA, RN, B1, B2, WD02, WD02_MAX, WD02_MIN, WS02, WS02_MAX, WS02_MIN, WD10, WD10_MAX, WD10_MIN, WS10, WS10_MAX, WS10_MIN])
    df = pd.DataFrame(records)
    df.to_csv('/tmp/airport_weather_infor.csv', encoding="utf-8-sig")


with DAG(
    dag_id='update_weather_infor',
    start_date=datetime(2024, 5, 31, 16, 4),  # 이 시간에서 8시간 55분을 더하면 202406011259 -> 202406011200 ~ 202406011259 데이터 가져오도록 유도
    schedule=timedelta(minutes=60),  
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        'on_failure_callback': slack.on_failure_callback,
    }
) as dag:

    api_key = Variable.get("weather_api_key")
    date_now = '{{ ts }}'
    

    upload_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id='upload_to_gcs', 
    src='/tmp/airport_weather_infor.csv',
    dst='source/weather/{{ execution_date.in_timezone("Asia/Seoul").strftime("%Y/%m/%d") }}/weather_infor_{{ execution_date.in_timezone("Asia/Seoul").strftime("%Y%m%d%H") }}.csv', 
    bucket='pdc3project-landing-zone-bucket',
    gcp_conn_id='google_cloud_GCS',
    dag=dag 
    )

    transform(extract(api_key, date_now)) >> upload_to_gcs_task