from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago

from plugins import slack

import numpy as np
import pandas as pd
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import statsmodels.api as sm

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'on_failure_callback': slack.on_failure_callback,
}

# 새로운 데이터를 Pandas DataFrame으로 로드
def load_new_data(**kwargs):
    hook = BigQueryHook(gcp_conn_id='google_cloud_bigquery', use_legacy_sql=False, location='asia-northeast3')
    sql = """
    WITH union_all_airports AS (
        SELECT AIRLINE_KOREAN, AIRPORT, ARRIVED_KOR, BOARDING_KOR, FLIGHT_DATE, IO, LINE, RMK_KOR, STD, ETD, UFID,
            DATETIME(CONCAT(CAST(FLIGHT_DATE AS STRING), ' ', 
            LPAD(CAST(FLOOR(ETD / 100) AS STRING), 2, '0') || ':' ||
            LPAD(CAST(MOD(ETD, 100) AS STRING), 2, '0') || ':00'
            )) AS departure_datetime,
            TIME_DIFF(
                PARSE_TIME('%H%M', LPAD(CAST(ETD AS STRING), 4, '0')),
                PARSE_TIME('%H%M', LPAD(CAST(STD AS STRING), 4, '0')),
                MINUTE
            ) AS DELAY_TIME
        FROM `pdc3project.raw_data.flight_data`
    ),
    airport_s AS (
        SELECT 
            uaa.*, 
            S,
        FROM union_all_airports uaa
        LEFT JOIN `pdc3project.raw_data.weather_airport_conn`
        ON AIRPORT_CODE = AIRPORT
    ),
    formatted_weather_cte AS (
        SELECT *, 
            DATETIME(
            SUBSTR(TM, 1, 4) || '-' || 
            SUBSTR(TM, 5, 2) || '-' || 
            SUBSTR(TM, 7, 2) || ' ' || 
            SUBSTR(TM, 9, 2) || ':' || 
            SUBSTR(TM, 11, 2) || ':00'
            ) AS weather_datetime
        FROM `pdc3project.raw_data.weather_infor`
    ),
    flight_weather_cte AS (
        SELECT *
        FROM airport_s aps
        JOIN formatted_weather_cte wi
        ON 
            aps.S = wi.S
            AND 
            wi.weather_datetime BETWEEN TIMESTAMP_SUB(aps.departure_datetime, INTERVAL 30 MINUTE) 
                        AND aps.departure_datetime
        )
    SELECT 
        CASE
        WHEN DELAY_TIME < 0 THEN '~0'
        WHEN DELAY_TIME BETWEEN 0 AND 10 THEN '0~10'
        WHEN DELAY_TIME BETWEEN 10 AND 20 THEN '10~20'
        WHEN DELAY_TIME BETWEEN 20 AND 30 THEN '20~30'
        WHEN DELAY_TIME > 30 THEN '30~'
        ELSE 'Unknown'
        END AS DELAY_CATEGORY,
        DELAY_TIME,
        WS10, HM, PA, TA,
        AIRLINE_KOREAN,
        TM
    FROM flight_weather_cte
    """
    df = hook.get_pandas_df(sql, dialect='standard')
    df['TM'] = pd.to_datetime(df['TM'], format='%Y%m%d%H%M')
    df['AIRLINE_KOREAN'].replace('nan', np.nan, inplace=True)
    df = df.dropna(subset=['AIRLINE_KOREAN'])

    execution_date = kwargs['execution_date']

    table_name = 'flight_weather'
    
    gcs_object_name = f'source/{ table_name }/{ execution_date.strftime("%Y/%m/%d") }/{ table_name }_data_{ execution_date.strftime("%Y%m%d") }.parquet'
    upload_to_gcs(df, gcs_object_name)

    bq_source_uris = f'gs://pdc3project-analytics-layer-bucket/{ gcs_object_name }'
    upload_to_bigquery(table_name, bq_source_uris)
    
    kwargs['ti'].xcom_push(key='gcs_object_name', value=gcs_object_name)

def calculate_correlation(**kwargs):
    gcs_object_name = kwargs['ti'].xcom_pull(key='gcs_object_name')
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_GCS')
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
        gcs_hook.download(
            bucket_name='pdc3project-analytics-layer-bucket',
            object_name=gcs_object_name,
            filename=temp_file.name
        )
        table = pq.read_table(temp_file.name)
        data = table.to_pandas()

    airline_groups = data.groupby('AIRLINE_KOREAN')
    correlation_results = []
    for airline, group in airline_groups:
        correlation_matrix = group[['DELAY_TIME', 'TA', 'HM', 'PA', 'WS10']].corr()
        correlation_series = correlation_matrix['DELAY_TIME'][1:]  # Exclude 'DELAY_TIME' itself
        correlation_series['AIRLINE_KOREAN'] = airline  # Add airline name
        correlation_results.append(correlation_series)

    correlation_df = pd.DataFrame(correlation_results).reset_index(drop=True)
    if '__index_level_0__' in correlation_df.columns:
        correlation_df.drop(columns=['__index_level_0__'], inplace=True)

    kwargs['ti'].xcom_push(key='correlation_data', value=correlation_df.to_json())

def perform_regression(airline_data):
    airline_data = airline_data.dropna(subset=['DELAY_TIME', 'TA', 'HM', 'PA', 'WS10'])

    airline_data['DELAY_TIME'] = airline_data['DELAY_TIME'].astype(float)

    weather_columns = ['TA', 'HM', 'PA', 'WS10']
    for col in weather_columns:
        airline_data[col] = airline_data[col].astype(float)

    X = airline_data[weather_columns]
    y = airline_data['DELAY_TIME']
    X = sm.add_constant(X)
    model = sm.OLS(y, X).fit()
    return model

def extract_regression_summary(model, airline_name):
    summary = model.summary2().tables[1]

    result = []
    for index, row in summary.iterrows():
        regression_info = {
            'AIRLINE_KOREAN': airline_name,
            'R_squared': model.rsquared,
            'Adj_R_squared': model.rsquared_adj,
            'F_statistic': model.fvalue,
            'Prob_F_statistic': model.f_pvalue,
            'Log_Likelihood': model.llf,
            'AIC': model.aic,
            'BIC': model.bic,
            'No_Observations': model.nobs,
            'Df_Residuals': model.df_resid,
            'Df_Model': model.df_model
        }
        
        # Add coefficients, standard errors, t-values, p-values, confidence intervals
        regression_info[f'x'] = index
        regression_info[f'coef'] = row['Coef.']
        regression_info[f'std_err'] = row['Std.Err.']
        regression_info[f't'] = row['t']
        regression_info[f'p_value'] = row['P>|t|']
        regression_info[f'conf_low'] = row['[0.025']
        regression_info[f'conf_high'] = row['0.975]']

        result.append(regression_info)
    
    return result

def regression_analysis(**kwargs):
    gcs_object_name = kwargs['ti'].xcom_pull(key='gcs_object_name')
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_GCS')
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
        gcs_hook.download(
            bucket_name='pdc3project-analytics-layer-bucket',
            object_name=gcs_object_name,
            filename=temp_file.name
        )
        table = pq.read_table(temp_file.name)
        data = table.to_pandas()
        
    airline_groups = data.groupby('AIRLINE_KOREAN')
    regression_results = []

    for airline, group in airline_groups:
        model = perform_regression(group)
        regression_info = extract_regression_summary(model, airline)
        regression_results.extend(regression_info)

    regression_df = pd.DataFrame(regression_results)

    regression_df.replace([np.inf, -np.inf], 'inf', inplace=True)
    regression_df.fillna('null', inplace=True)
    regression_df = regression_df.astype(str)

    if '__index_level_0__' in regression_df.columns:
        regression_df.drop(columns=['__index_level_0__'], inplace=True)

    kwargs['ti'].xcom_push(key='regression_data', value=regression_df.to_json())

def store_final_table(**kwargs):
    execution_date = kwargs['execution_date']

    data = {
        'correlation_data': 'airline_weather_corr',
        'regression_data': 'airline_weather_regr'
    }
    for key, table_name in data.items():
        json_data = kwargs['ti'].xcom_pull(key=key)
        anal_result = pd.read_json(json_data)

        gcs_object_name = f'source/{ table_name }/{ execution_date.strftime("%Y/%m/%d") }/{ table_name }_data_{ execution_date.strftime("%Y%m%d") }.parquet'
        upload_to_gcs(anal_result, gcs_object_name)

        bq_source_uris = f'gs://pdc3project-analytics-layer-bucket/{ gcs_object_name }'
        upload_to_bigquery(table_name, bq_source_uris)

def upload_to_gcs(data, gcs_object_name):
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
        table = pa.Table.from_pandas(data)
        pq.write_table(table, temp_file.name, coerce_timestamps='us', use_deprecated_int96_timestamps=True)
        temp_file.flush()
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_GCS')
        gcs_hook.upload(
            bucket_name='pdc3project-analytics-layer-bucket',
            object_name=gcs_object_name,
            filename=temp_file.name
        )

def upload_to_bigquery(table_name, bq_source_uris):
    hook = BigQueryHook(gcp_conn_id='google_cloud_bigquery', location='asia-northeast3')
    hook.run_load(
        destination_project_dataset_table=f"pdc3project.analytics.{ table_name }",
        source_uris=[bq_source_uris],
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True
    )


with DAG(
    'airline_delay_corr_weather_analysis',
    description='Analyze weather impact on flight delays and store results in GCS, BigQuery',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    load_new_data_task = PythonOperator(
        task_id='load_new_data_task',
        python_callable=load_new_data,
    )

    calculate_correlation_task = PythonOperator(
        task_id='calculate_correlation_task',
        python_callable=calculate_correlation,
    )

    regression_analysis_task = PythonOperator(
        task_id='regression_analysis_task',
        python_callable=regression_analysis,
    )

    store_task = PythonOperator(
        task_id='store_task',
        python_callable=store_final_table,
    )

    load_new_data_task >> [calculate_correlation_task, regression_analysis_task] >> store_task