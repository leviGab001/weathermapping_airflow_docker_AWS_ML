from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import requests
import json
import os
from datetime import datetime
import time
import pandas as pd

from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from joblib import dump

# Setup & Configuration
API_URL = "https://api.openweathermap.org/data/2.5/weather"
API_KEY = "YOUR_API_KEY"
CITIES = ['berlin', 'tel aviv', 'istanbul']
PATH_RAW = '/app/raw_files'
PATH_CLEAN = '/app/clean_data'

PATH_MODEL = './'
FULL_DATASET_PATH = PATH_CLEAN + '/fulldata.csv'
BEST_MODEL_PATH = PATH_CLEAN + '/best_model.pickle'

weather_api_dag = DAG(
    dag_id='retrieve_weather_data_from_api',
    description='This DAG retrieves data from the OpenWeatherMap',
    tags=['evaluation', 'datascientest'],
    schedule_interval='* * * * *',
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1),
    },
    catchup=False
)

def fetch_weather_data(city):
    params = {
        "q": city,
        "appid": API_KEY
    }
    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    return response.json()

def retrieve_and_save_weather_data():
    all_data = []

    # Data Retrieval
    for city in CITIES:
        try:
            city_data = fetch_weather_data(city)
            all_data.append(city_data)
        except requests.RequestException as e:
            print(f"Error fetching data for {city}: {e}")

    # now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    now_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    filename = f"{now_time}.json"
    filepath = os.path.join(PATH_RAW, filename)

    with open(filepath, 'w') as f:
        json.dump(all_data, f, indent=4)

    print(f"Data saved to {filepath}")

def transform_data_into_csv(n_files=None, filename='data.csv'):
    parent_folder = PATH_RAW
    files = sorted(os.listdir(PATH_RAW), reverse=True)
    files = [f for f in files if f[0].isdigit()]

    if n_files:
        files = files[:n_files]

    dfs = []

    for f in files:
        with open(os.path.join(parent_folder, f), 'r') as file:
            print(file)
            data_temp = json.load(file)
        for data_city in data_temp:
            dfs.append(
                {
                    'temperature': data_city['main']['temp'],
                    'city': data_city['name'],
                    'pression': data_city['main']['pressure'],
                    'date': f.split('.')[0]
                }
            )

    df = pd.DataFrame(dfs)

    df.to_csv(os.path.join(PATH_CLEAN, filename), index=False)


def compute_model_score(model, X, y):
    # computing cross val
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    model_score = cross_validation.mean()

    return model_score


def train_and_save_model(model, X, y, path_to_model='./app/model.pckl'):
    model.fit(X, y)
    print(str(model), 'saved at ', path_to_model)
    dump(model, path_to_model)


def prepare_data(path_to_data=FULL_DATASET_PATH):
    df = pd.read_csv(path_to_data)
    df = df.sort_values(['city', 'date'], ascending=True)

    dfs = []

    for c in df['city'].unique():
        # df_temp = df[df['city'] == c]
        df_temp = df[df['city'] == c].copy()

        # creating target
        df_temp.loc[:, 'target'] = df_temp['temperature'].shift(1)

        # creating features
        for i in range(1, 10):
            df_temp.loc[:, 'temp_m-{}'.format(i)
                        ] = df_temp['temperature'].shift(-i)

        # deleting null values
        df_temp = df_temp.dropna()

        dfs.append(df_temp)

    # concatenating datasets
    df_final = pd.concat(
        dfs,
        axis=0,
        ignore_index=False
    )

    df_final = df_final.drop(['date'], axis=1)

    df_final = pd.get_dummies(df_final)

    features = df_final.drop(['target'], axis=1)
    target = df_final['target']

    return features, target

def compute_score_and_save(model_name, task_instance):
    X, y = prepare_data(FULL_DATASET_PATH)

    model_mapping = {
        "LinearRegression": LinearRegression(),
        "DecisionTreeRegressor": DecisionTreeRegressor(),
        "RandomForestRegressor": RandomForestRegressor(),
    }

    score = compute_model_score(model_mapping[model_name], X, y)

    task_instance.xcom_push(
        key=model_name,
        value=score
    )


def choose_model_and_train(task_instance):
    X, y = prepare_data(FULL_DATASET_PATH)

    model_mapping = {
        "LinearRegression": LinearRegression(),
        "DecisionTreeRegressor": DecisionTreeRegressor(),
        "RandomForestRegressor": RandomForestRegressor(),
    }

    model_performance = {
        "LinearRegression": task_instance.xcom_pull(key="LinearRegression",task_ids=['train_model_lr']),
        "DecisionTreeRegressor": task_instance.xcom_pull(key="DecisionTreeRegressor",task_ids=['train_model_dtr']),
        "RandomForestRegressor": task_instance.xcom_pull(key="RandomForestRegressor",task_ids=['train_model_rfr']),
    }

    model_performance = {k: v for k, v in model_performance.items() if v is not None}

    if not model_performance:
        raise ValueError("All models resulted in None performance scores.")

    best_model = max(model_performance, key=model_performance.get)

    train_and_save_model(model_mapping[best_model], X, y, BEST_MODEL_PATH)


####################################################################
####################################################################


task1 = PythonOperator(
    task_id='task_retrieve_weather_data',
    dag=weather_api_dag,
    python_callable=retrieve_and_save_weather_data
)


task2 = PythonOperator(
    task_id='transform_to_csv_last_20_json',
    dag=weather_api_dag,
    python_callable=transform_data_into_csv,
    op_kwargs={'n_files': 20},
)

task3 = PythonOperator(
    task_id='transform_to_csv_all_json',
    dag=weather_api_dag,
    python_callable=transform_data_into_csv,
    op_kwargs={'filename': 'fulldata.csv'},
)


task4_lr = PythonOperator(
    task_id='train_model_lr',
    dag=weather_api_dag,
    python_callable=compute_score_and_save,
    op_kwargs={'model_name': 'LinearRegression'},
)

task4_dtr = PythonOperator(
    task_id='train_model_dtr',
    dag=weather_api_dag,
    python_callable=compute_score_and_save,
    op_kwargs={'model_name': 'DecisionTreeRegressor'},
)

task4_rfr = PythonOperator(
    task_id='train_model_rfr',
    dag=weather_api_dag,
    python_callable=compute_score_and_save,
    op_kwargs={'model_name': 'RandomForestRegressor'},
)

task5 = PythonOperator(
    task_id='select_best_model',
    dag=weather_api_dag,
    python_callable=choose_model_and_train,
)


task1 >> [task2, task3]
[task4_lr, task4_dtr, task4_rfr] >> task5
