import os
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
RAW_FOLDER_PATH = '/app/raw_files'
CLEAN_FOLDER_PATH  = '/app/clean_data'
DATASET_PATH = '/app/clean_data/latest_data.csv'
BEST_MODEL_PATH = '/app/clean_data/best_model.pickle'
BUCKET = 'weathermapping-bucket'

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
    filepath = os.path.join(RAW_FOLDER_PATH, filename)

    try:
        with open(filepath, 'w') as f:
            json.dump(all_data, f, indent=4)
        logging.info(f"Data saved to {filepath}")
    except IOError as e:
        logging.error(f"File IO Error: {e}")

# Function to transform data into CSV
def transform_data_into_csv():
    parent_folder = RAW_FOLDER_PATH
    files = sorted([f for f in os.listdir(parent_folder) if f.endswith('.json')], reverse=True)

    dfs = []
    for f in files:
        try:
            with open(os.path.join(parent_folder, f), 'r') as file:
                data_temp = json.load(file)
                for data_city in data_temp:
                    temperature_kelvin = data_city['main']['temp']
                    temperature_celsius = temperature_kelvin - 273.15  # Kelvin to Celsius
                    dfs.append({
                        'temperature': round(temperature_celsius, 2),
                        'city': data_city['name'],
                        'pressure': data_city['main']['pressure'],
                        'date': f.split('.')[0]
                    })
        except (IOError, json.JSONDecodeError) as e:
            logging.error(f"Error processing file {f}: {e}")

    if not dfs:
        logging.warning("No data to process.")
        return

    df = pd.DataFrame(dfs)
    os.makedirs(CLEAN_FOLDER_PATH , exist_ok=True)
    full_data_path = os.path.join(CLEAN_FOLDER_PATH , 'full_data.csv')
    df.to_csv(full_data_path, index=False)
    logging.info(f"All data saved to {full_data_path}.")

    df_grouped = df.groupby('city').apply(lambda x: x.sort_values('date', ascending=False).head(20)).reset_index(drop=True)
    latest_data_path = os.path.join(CLEAN_FOLDER_PATH , 'latest_data.csv')
    df_grouped.to_csv(latest_data_path, index=False)
    logging.info(f"Latest 20 records for each city saved to {latest_data_path}.")

# Functions for model training and selection
def compute_model_score(model, X, y):
    scores = cross_val_score(model, X, y, cv=3, scoring='neg_mean_squared_error')
    return -scores.mean()

def train_and_save_model(model, X, y, path_to_model):
    model.fit(X, y)
    dump(model, path_to_model)
    logging.info(f'{str(model)} saved at {path_to_model}')

def prepare_data(path_to_data=DATASET_PATH):
    df = pd.read_csv(path_to_data)
    df.sort_values(['city', 'date'], ascending=True, inplace=True)

    dfs = []
    for c in df['city'].unique():
        df_temp = df[df['city'] == c].copy()

        # Creating target and features
        df_temp['target'] = df_temp['temperature'].shift(1)
        for i in range(1, 10):
            df_temp[f'temp_m-{i}'] = df_temp['temperature'].shift(-i)

        # Deleting rows with null values
        df_temp.dropna(inplace=True)
        dfs.append(df_temp)

    # Concatenating datasets
    df_final = pd.concat(dfs, axis=0, ignore_index=True)
    df_final.drop(['date'], axis=1, inplace=True)

    # Creating dummies
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

    # Retrieving scores from previous tasks
    model_performance = {
        "LinearRegression": task_instance.xcom_pull(key="LinearRegression", task_ids=['train_linear_regression_model'])[0],
        "DecisionTreeRegressor": task_instance.xcom_pull(key="DecisionTreeRegressor", task_ids=['train_decision_tree_model'])[0],
        "RandomForestRegressor": task_instance.xcom_pull(key="RandomForestRegressor", task_ids=['train_random_forest_model'])[0],
    }

    # Filtering out None values
    model_performance = {k: v for k, v in model_performance.items() if v is not None}

    # Validating model performance data
    if not model_performance:
        raise ValueError("All models resulted in None performance scores.")

    # Selecting the best model
    best_model = max(model_performance, key=model_performance.get)

    # Training and saving the best model
    train_and_save_model(model_mapping[best_model], X, y, BEST_MODEL_PATH)

# Apache Airflow DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('weathermap_data_pipeline', 
          start_date= days_ago(1),
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

def upload_to_s3() -> None:
    hook = S3Hook('aws_conn')
    hook.load_file(
        filename=BEST_MODEL_PATH,
        key='best_model.pickle',
        bucket_name=BUCKET,
        replace=True
)

task_upload_to_s3 = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3
)

fetch_data_task >> transform_data_task >> [train_linear_regression_model_task, 
                                           train_decision_tree_model_task, 
                                           train_random_forest_model_task] >> select_best_model_task
select_best_model_task >> task_upload_to_s3