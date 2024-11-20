from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
from ETL import transform_data  # Import your functions

# Define DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'transform_tweets',
    default_args=default_args,
    description='Transform tweets after all seasonal data is scraped',
    schedule_interval=None,  # Triggered by the Extract DAG
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task 3: Apply transformations to the concatenated file
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=lambda: transform_data(
            input_filename='{file input}',
            output_filename='{file output}'
        ),
    )