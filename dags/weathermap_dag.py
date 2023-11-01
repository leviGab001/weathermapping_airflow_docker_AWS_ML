from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import json
import os

# Constants
WEATHER_API_ENDPOINT = '/data/2.5/weather?q=Portland&APPID='
S3_BUCKET = ''

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email': ['levigab001@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

def kelvin_to_fahrenheit(temp_in_kelvin):
    """Converts Kelvin to Fahrenheit."""
    return (temp_in_kelvin - 273.15) * 9/5 + 32

def transform_load_data(task_instance):
    """Transforms and loads weather data."""
    data = task_instance.xcom_pull(task_ids="extract_weather_data")

    # Data transformation
    transformed_data = {
        "City": data["name"],
        "Description": data["weather"][0]['description'],
        "Temperature (F)": kelvin_to_fahrenheit(data["main"]["temp"]),
        "Feels Like (F)": kelvin_to_fahrenheit(data["main"]["feels_like"]),
        "Min Temp (F)": kelvin_to_fahrenheit(data["main"]["temp_min"]),
        "Max Temp (F)": kelvin_to_fahrenheit(data["main"]["temp_max"]),
        "Pressure": data["main"]["pressure"],
        "Humidity": data["main"]["humidity"],
        "Wind Speed": data["wind"]["speed"],
        "Time of Record": datetime.utcfromtimestamp(data['dt'] + data['timezone']),
        "Sunrise (Local Time)": datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone']),
        "Sunset (Local Time)": datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])
    }

    # Convert to DataFrame
    df_data = pd.DataFrame([transformed_data])

    # Load data - saving to S3
    aws_credentials = {
        "key": os.getenv('AWS_ACCESS_KEY_ID'),
        "secret": os.getenv('AWS_SECRET_ACCESS_KEY'),
        "token": os.getenv('AWS_SESSION_TOKEN')
    }
    s3_bucket = 'your-s3-bucket-name'
    filename = 'current_weather_data_portland_' + datetime.now().strftime("%d%m%Y%H%M%S") + '.csv'
    s3_path = f"s3://{s3_bucket}/{filename}"

    # Upload to S3
    df_data.to_csv(s3_path, index=False, storage_options=aws_credentials)


with DAG('weather_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint=WEATHER_API_ENDPOINT
    )

    extract_weather_data = SimpleHttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weathermap_api',
        endpoint=WEATHER_API_ENDPOINT,
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True
    )

    transform_load_weather_data = PythonOperator(
        task_id='transform_load_weather_data',
        python_callable=transform_load_data
    )

    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data
