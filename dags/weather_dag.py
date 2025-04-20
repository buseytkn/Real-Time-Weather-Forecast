from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from veri import fetch_weather_data
from model import streamlit_app

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
} 

dag = DAG(
    'current_weather_data',
    default_args=default_args,
    description='Fetch current weather data',
    schedule_interval=timedelta(hours=4),
    catchup=False,
)

fetch_hourly_task = PythonOperator(
    task_id = 'fetch_current_weather_data_task',
    python_callable= fetch_weather_data,
    depends_on_past=True,
    dag = dag
)

train_model_task = PythonOperator(
    task_id = "model_and_streamlit_task",
    python_callable = streamlit_app,
    dag = dag
)

fetch_hourly_task >> train_model_task 