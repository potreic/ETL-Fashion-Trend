from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from ETL import transform_data  # Import your transform function

def run_transformation(**kwargs):
    input_filename = kwargs['dag_run'].conf.get('input_filename', 'default_input')
    output_filename = kwargs['dag_run'].conf.get('output_filename', input_filename)  # Default to the same name
    transform_data(
        input_filename=input_filename,
        output_filename=output_filename
    )

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
    description='Transform tweets after extraction',
    schedule_interval=None,  # Triggered by the Extract DAG
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task: Apply transformations
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=run_transformation,
        provide_context=True,  # Ensure we can access `dag_run.conf`
    )
