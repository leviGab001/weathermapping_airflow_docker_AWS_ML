import os
import io
import json
import requests
import pandas as pd
import logging
from datetime import datetime, timedelta
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from joblib import dump
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.S3_hook import S3Hook


# Configuration for API and file paths
API_URL = os.environ.get('OPENWEATHER_API_URL', "https://api.openweathermap.org/data/2.5/weather")
API_KEY = os.environ['OPENWEATHER_API_KEY']
CITIES = ['berlin', 'paris', 'london']
BUCKET = 'weathermapping-bucket'
RAW_BUCKET_PATH = 's3://weathermapping-bucket/raw_data'
PROCESSED_BUCKET_PATH = 's3://weathermapping-bucket/processed_data'
BEST_MODEL_KEY = 'best_model.pickle'
FULL_DATASET_KEY = 'processed_data/full_dataset.csv'
LATEST_DATA_KEY = 'processed_data/latest_data.csv'
RAW_DATA_PREFIX = 'raw_data/'

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to fetch and save weather data
def fetch_and_save_weather_data():
    all_data = []
    for city in CITIES:
        params = {"q": city, "appid": API_KEY}
        try:
            response = requests.get(API_URL, params=params)
            response.raise_for_status()
            city_data = response.json()
            if city_data:
                all_data.append(city_data)
        except requests.HTTPError as http_err:
            logging.error(f"HTTP error occurred for {city}: {http_err}")
        except Exception as err:
            logging.error(f"Other error occurred for {city}: {err}")

    now_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    filename = f"{now_time}.json"
    local_filepath = os.path.join('/tmp', filename)

    try:
        with open(local_filepath, 'w') as f:
            json.dump(all_data, f, indent=4)
        s3_hook = S3Hook('aws_conn')
        s3_hook.load_file(local_filepath, key=f'raw_data/{filename}', bucket_name=BUCKET, replace=True)
        logging.info(f"Data uploaded to S3 at {RAW_BUCKET_PATH}/{filename}")
    except Exception as e:
        logging.error(f"Error: {e}")

def transform_data_into_csv():
    s3_hook = S3Hook('aws_conn')

    # List and sort raw data files in S3
    list_of_files = s3_hook.list_keys(bucket_name=BUCKET, prefix=RAW_DATA_PREFIX)
    if not list_of_files:
        logging.warning("No raw data files found in S3.")
        return

    dfs = []
    for file_key in sorted(list_of_files, reverse=True):
        # Read each file directly into memory
        raw_data = s3_hook.read_key(key=file_key, bucket_name=BUCKET)
        raw_data_io = io.StringIO(raw_data)

        try:
            data_temp = json.load(raw_data_io)
            for data_city in data_temp:
                temperature_kelvin = data_city['main']['temp']
                temperature_celsius = temperature_kelvin - 273.15  # Kelvin to Celsius
                dfs.append({
                    'temperature': round(temperature_celsius, 2),
                    'city': data_city['name'],
                    'pressure': data_city['main']['pressure'],
                    'date': os.path.basename(file_key).split('.')[0]
                })
        except json.JSONDecodeError as e:
            logging.error(f"Error processing file {os.path.basename(file_key)}: {e}")

    if not dfs:
        logging.warning("No data to process.")
        return

    df = pd.DataFrame(dfs)

    # Convert full dataset DataFrame to CSV and upload to S3
    full_csv_buffer = io.StringIO()
    df.to_csv(full_csv_buffer, index=False)
    full_csv_buffer.seek(0)
    s3_hook.load_string(string_data=full_csv_buffer.getvalue(), key=FULL_DATASET_KEY, bucket_name=BUCKET, replace=True)
    logging.info("Full dataset uploaded to S3.")

    # Process latest 20 records for each city
    df_grouped = df.groupby('city').apply(lambda x: x.sort_values('date', ascending=False).head(20)).reset_index(drop=True)

    # Convert latest data DataFrame to CSV and upload to S3
    latest_csv_buffer = io.StringIO()
    df_grouped.to_csv(latest_csv_buffer, index=False)
    latest_csv_buffer.seek(0)
    s3_hook.load_string(string_data=latest_csv_buffer.getvalue(), key=LATEST_DATA_KEY, bucket_name=BUCKET, replace=True)
    logging.info("Latest data for each city uploaded to S3.")



# Functions for model training and selection
def compute_model_score(model, X, y):
    scores = cross_val_score(model, X, y, cv=3, scoring='neg_mean_squared_error')
    return -scores.mean()

def train_and_save_model(model, X, y):
    model.fit(X, y)

    local_model_path = '/tmp/best_model.pickle'
    dump(model, local_model_path)
    s3_hook = S3Hook('aws_conn')
    s3_hook.load_file(local_model_path, key=BEST_MODEL_KEY, bucket_name=BUCKET, replace=True)
    logging.info(f'Model uploaded to S3 at {PROCESSED_BUCKET_PATH}/{BEST_MODEL_KEY}')

def prepare_data():
    s3_hook = S3Hook('aws_conn')
    latest_data_key = LATEST_DATA_KEY

    # Read latest data CSV directly from S3 into DataFrame
    csv_data = s3_hook.read_key(key=latest_data_key, bucket_name=BUCKET)
    csv_buffer = io.StringIO(csv_data)
    df = pd.read_csv(csv_buffer)
    df.sort_values(['city', 'date'], ascending=True, inplace=True)

    dfs = []
    for c in df['city'].unique():
        df_temp = df[df['city'] == c].copy()
        df_temp['target'] = df_temp['temperature'].shift(1)
        for i in range(1, 10):
            df_temp[f'temp_m-{i}'] = df_temp['temperature'].shift(-i)
        df_temp.dropna(inplace=True)
        dfs.append(df_temp)

    df_final = pd.concat(dfs, axis=0, ignore_index=True)
    df_final.drop(['date'], axis=1, inplace=True)
    df_final = pd.get_dummies(df_final)
    features = df_final.drop(['target'], axis=1)
    target = df_final['target']

    return features, target



def compute_score_and_save(model_name, task_instance):
    X, y = prepare_data()
    model_mapping = {
        "LinearRegression": LinearRegression(),
        "DecisionTreeRegressor": DecisionTreeRegressor(),
        "RandomForestRegressor": RandomForestRegressor(),
    }

    score = compute_model_score(model_mapping[model_name], X, y)
    task_instance.xcom_push(key=model_name, value=score)


def choose_model_and_train(task_instance):
    X, y = prepare_data()
    model_mapping = {
        "LinearRegression": LinearRegression(),
        "DecisionTreeRegressor": DecisionTreeRegressor(),
        "RandomForestRegressor": RandomForestRegressor(),
    }

    model_performance = {
        "LinearRegression": task_instance.xcom_pull(key="LinearRegression", task_ids=['train_linear_regression_model'])[0],
        "DecisionTreeRegressor": task_instance.xcom_pull(key="DecisionTreeRegressor", task_ids=['train_decision_tree_model'])[0],
        "RandomForestRegressor": task_instance.xcom_pull(key="RandomForestRegressor", task_ids=['train_random_forest_model'])[0],
    }

    model_performance = {k: v for k, v in model_performance.items() if v is not None}

    if not model_performance:
        raise ValueError("All models resulted in None performance scores.")

    best_model = max(model_performance, key=model_performance.get)
    train_and_save_model(model_mapping[best_model], X, y)


# Apache Airflow DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('weathermap_data_pipeline', 
          start_date=days_ago(1),
          default_args=default_args, 
          description='A DAG for processing weather data and training models', 
          schedule_interval='*/30 * * * *', 
          catchup=False)

fetch_data_task = PythonOperator(task_id='fetch_and_save_weather_data', 
                                 python_callable=fetch_and_save_weather_data, 
                                 dag=dag)

transform_data_task = PythonOperator(task_id='transform_data_into_csv', 
                                     python_callable=transform_data_into_csv, 
                                     dag=dag)

train_linear_regression_model_task = PythonOperator(task_id='train_linear_regression_model', 
                                                    python_callable=compute_score_and_save, 
                                                    op_kwargs={'model_name': 'LinearRegression'}, 
                                                    dag=dag)

train_decision_tree_model_task = PythonOperator(task_id='train_decision_tree_model', 
                                                python_callable=compute_score_and_save, 
                                                op_kwargs={'model_name': 'DecisionTreeRegressor'}, 
                                                dag=dag)

train_random_forest_model_task = PythonOperator(task_id='train_random_forest_model', 
                                                python_callable=compute_score_and_save, 
                                                op_kwargs={'model_name': 'RandomForestRegressor'}, 
                                                dag=dag)

select_best_model_task = PythonOperator(task_id='select_best_model', 
                                        python_callable=choose_model_and_train, 
                                        dag=dag)

fetch_data_task >> transform_data_task >> [train_linear_regression_model_task, 
                                           train_decision_tree_model_task, 
                                           train_random_forest_model_task] >> select_best_model_task




