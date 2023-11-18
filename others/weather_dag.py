from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import os
import pandas as pd
import logging
from decouple import *  # Import the config function from decouple

# Load configuration from the .env file
API_KEY = config('API_KEY')
CITIES = config('CITIES').split(',')  # Split the comma-separated string into a list
RAW_FOLDER_PATH = config('RAW_FOLDER_PATH')
CLEAN_FOLDER_PATH = config('CLEAN_FOLDER_PATH')

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
            logging.error(f"Error fetching data for {city}: {response.status_code}")

    # Rest of the code for saving data...

def transform_data_into_csv(n_files=None, input_folder=RAW_FOLDER_PATH, output_folder=CLEAN_FOLDER_PATH, filename='data.csv'):
    """
    Function to transform JSON weather data into CSV format.

    Args:
        n_files (int): Number of recent files to process (default is None for all files).
        input_folder (str): Folder containing JSON files.
        output_folder (str): Folder to save transformed data.
        filename (str): Name of the output CSV file.
    """

# Create a DAG instance
dag = DAG(
    dag_id='weather_data_pipeline',
    tags=['weather_tag'],
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 10, 30),
        'retries': 1,
    },
    description='A DAG to fetch weather data and save it as JSON',
    schedule_interval=None,
    catchup=False
)

# Create PythonOperator tasks
fetch_and_save_weather_task = PythonOperator(
    task_id='fetch_and_save_weather_task',
    python_callable=fetch_and_save_weather_data,
    op_args=[CITIES, API_KEY, RAW_FOLDER_PATH],
    provide_context=True,
    dag=dag,
)

task_transform_to_csv_all_v2 = PythonOperator(
    task_id='transform_to_csv_all_v2',
    python_callable=transform_data_into_csv,
    op_args=[None, RAW_FOLDER_PATH, CLEAN_FOLDER_PATH, 'fulldata.csv'],
    dag=dag
)

# Define the task dependencies
fetch_and_save_weather_task >> task_transform_to_csv_all_v2
