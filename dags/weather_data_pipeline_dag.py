from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from extract import fetch_and_save_weather_data
from transform import transform_data_into_csv
from train_model import (
    prepare_data, compute_model_score, train_and_save_model, 
    compute_score_and_save, choose_model_and_train
)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='A DAG for processing weather data and training models',
    schedule_interval=timedelta(days=1),
)

# Task to fetch and save weather data
fetch_data_task = PythonOperator(
    task_id='fetch_and_save_weather_data',
    python_callable=fetch_and_save_weather_data,
    dag=dag,
)

# Task to transform data into CSV
transform_data_task = PythonOperator(
    task_id='transform_data_into_csv',
    python_callable=transform_data_into_csv,
    dag=dag,
)

train_linear_regression_model_task = PythonOperator(
    task_id='train_linear_regression_model',
    dag=dag,
    python_callable=compute_score_and_save,
    op_kwargs={'model_name': 'LinearRegression'},
)

train_decision_tree_model_task = PythonOperator(
    task_id='train_decision_tree_model',
    dag=dag,
    python_callable=compute_score_and_save,
    op_kwargs={'model_name': 'DecisionTreeRegressor'},
)

train_random_forest_model_task = PythonOperator(
    task_id='train_random_forest_model',
    dag=dag,
    python_callable=compute_score_and_save,
    op_kwargs={'model_name': 'RandomForestRegressor'},
)

select_best_model_task = PythonOperator(
    task_id='select_best_model',
    dag=dag,
    python_callable=choose_model_and_train,
)


# Task dependencies
fetch_data_task >> transform_data_task >> [
    train_linear_regression_model_task,
    train_decision_tree_model_task,
    train_random_forest_model_task
] >> select_best_model_task