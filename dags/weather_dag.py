from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import os
import pandas as pd

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 30),
    'retries': 1,
}

# Create a DAG instance
dag = DAG(
    dag_id='weather_data_pipeline',
    tags=['weather_tag'],
    default_args=default_args,
    description='A DAG to fetch weather data and save it as JSON',
    schedule_interval=None,  # You can specify a schedule interval here
    catchup=False  # Set to False if you don't want to catch up on missed runs
)

# API key and cities for weather data retrieval
api_key = "873d885e4c75a5107f10d76d8b4057a1"
cities = ['paris', 'london', 'washington']
folder_path = "/app/raw_files"

# Task 1: Fetch and Save Weather Data
def fetch_and_save_weather_data(cities, api_key, folder_path, **kwargs):
    """
    Function to fetch weather data for specified cities and save it as JSON files.
    
    Args:
        cities (list): List of city names.
        api_key (str): API key for OpenWeatherMap API.
        folder_path (str): Path where JSON files will be saved.
    
    Keyword Args:
        kwargs: Additional keyword arguments passed by Airflow.
    """
    base_url = "https://api.openweathermap.org/data/2.5/weather"
    weather_data = []

    for city in cities:
        params = {
            'q': city,
            'appid': api_key
        }
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            weather_data.append(response.json())
        else:
            print(f"Error fetching data for {city}: {response.status_code}")

    # Check if the folder exists, create it if not
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    file_name = f"{timestamp}.json"
    file_path = os.path.join(folder_path, file_name)

    with open(file_path, 'w') as file:
        json.dump(weather_data, file)

    print(f"Data saved to {file_path}")

# Create a PythonOperator for Task 1
fetch_and_save_weather_task = PythonOperator(
    task_id='fetch_and_save_weather_task',
    python_callable=fetch_and_save_weather_data,
    op_args=[cities, api_key, folder_path],
    provide_context=True,
    dag=dag,
)

# Task 2: Transform Data into CSV and Pickle
def transform_data_into_csv_and_pickle(n_files=None, input_folder='/app/raw_files', output_folder='/app/clean_data', filename='data.csv'):
    """
    Function to transform JSON weather data into CSV and Pickle formats.
    
    Args:
        n_files (int): Number of recent files to process (default is None for all files).
        input_folder (str): Folder containing JSON files.
        output_folder (str): Folder to save transformed data.
        filename (str): Name of the output CSV file.
    """
    files = sorted(os.listdir(input_folder), reverse=True)
    if n_files:
        files = files[:n_files]

    dfs = []
    for f in files:
        with open(os.path.join(input_folder, f), 'r') as file:
            data_temp = json.load(file)
        for data_city in data_temp:
            dfs.append({
                'temperature': data_city['main']['temp'],
                'city': data_city['name'],
                'pressure': data_city['main']['pressure'],
                'date': f.split('.')[0]
            })

    df = pd.DataFrame(dfs)
    
    # Save as CSV
    csv_path = os.path.join(output_folder, filename)
    df.to_csv(csv_path, index=False)

    # Save as Pickle
    pickle_path = os.path.join(output_folder, filename.replace('.csv', '.pkl'))
    df.to_pickle(pickle_path)

# Update the task to use the new function for Task 2
task_transform_to_csv_20 = PythonOperator(
    task_id='transform_to_csv_20',
    python_callable=transform_data_into_csv_and_pickle,
    op_args=[20, folder_path, "/app/clean_data", 'data.csv'],
    dag=dag
)

task_transform_to_csv_all = PythonOperator(
    task_id='transform_to_csv_all',
    python_callable=transform_data_into_csv_and_pickle,
    op_args=[None, folder_path, "/app/clean_data", 'fulldata.csv'],
    dag=dag
)




# Define the task dependencies
fetch_and_save_weather_task >> [task_transform_to_csv_20, task_transform_to_csv_all]

task_transform_to_csv_all >> task_prepare_data

task_prepare_data >> [task_train_linear_regression, task_train_decision_tree, task_train_random_forest]

[task_train_linear_regression, task_train_decision_tree, task_train_random_forest] >> task_select_best_model

task_select_best_model >> task_train_best_model
