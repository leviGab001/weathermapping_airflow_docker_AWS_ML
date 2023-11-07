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

def transform_data_into_csv(n_files=None, input_folder='/app/raw_files', output_folder='/app/clean_data', filename='data.csv'):
    """
    Function to transform JSON weather data into CSV format.

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
        file_path = os.path.join(input_folder, f)
        
        # Check if the file is empty
        if os.path.getsize(file_path) == 0:
            print(f"Skipping empty file: {file_path}")
            continue

        with open(file_path, 'r') as file:
            file_content = file.read()
            try:
                data_temp = json.loads(file_content)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON data in file: {file_path}")
                print(f"Error details: {str(e)}")
                continue

        if not data_temp:
            print(f"Skipping empty JSON data in file: {file_path}")
            continue

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

# Create a PythonOperator for Task 1
fetch_and_save_weather_task = PythonOperator(
    task_id='fetch_and_save_weather_task',
    python_callable=fetch_and_save_weather_data,
    op_args=[cities, api_key, folder_path],
    provide_context=True,
    dag=dag,
)


task_transform_to_csv_all_v2 = PythonOperator(
    task_id='transform_to_csv_all_v2',
    python_callable=transform_data_into_csv,
    op_args=[None, folder_path, "/app/clean_data", 'fulldata.csv'],
    dag=dag
)

# Define the task dependencies
fetch_and_save_weather_task >> task_transform_to_csv_all_v2

