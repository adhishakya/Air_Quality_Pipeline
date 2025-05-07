from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from pipeline.extract import extract_data
from pipeline.transform import transform_data
from pipeline.load import load_data

default_args ={
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'air_quality_dag',
    default_args = default_args,
    description = 'ETL pipeline for fetching, transforming, and loading air quality data',
    schedule_interval = timedelta(hours = 1),
    catchup = False,
)as dag:
    extract = PythonOperator(
        task_id = 'extract_data',
        python_callable = extract_data,
    )

    transform = PythonOperator(
        task_id = 'transform_data',
        python_callable = transform_data,
    )

    load = PythonOperator(
        task_id = 'load_data',
        python_callable = load_data,
    )

    extract >> transform >> load


