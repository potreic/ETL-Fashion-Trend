from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from ETL import load_data, plot_data
import pandas as pd

def run_load_and_plot(**kwargs):
    # Retrieve configuration from dag_run.conf
    input_filename = kwargs['dag_run'].conf.get('input_filename', 'default_file')
    table_name = kwargs['dag_run'].conf.get('table_name', 'default_table')
    keyword = kwargs['dag_run'].conf.get('keyword', 'default_keyword')

    # Load the integrated CSV file into a DataFrame
    file_path = f'/home/rekdat/combined-data/{input_filename}.csv'
    df = pd.read_csv(file_path)

    # Load data into PostgreSQL
    load_data(df, table_name)

    # Generate and save the plot
    plot_data(df, keyword)

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
    'load_and_plot_data',
    default_args=default_args,
    description='Load integrated data to PostgreSQL and generate growth plots',
    schedule_interval=None,  # Triggered by the integration DAG
    start_date=datetime(2024, 11, 20),
    catchup=False,
) as dag:

    # Task: Load and Plot Data
    load_and_plot_task = PythonOperator(
        task_id='load_and_plot',
        python_callable=run_load_and_plot,
        provide_context=True,  # To access `dag_run.conf`
    )
