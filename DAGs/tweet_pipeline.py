from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from ETL import extract_tweet_all

def run_extraction(**kwargs):
    extract_tweet_all(
        token='{REDACTED}', 
        keyword=kwargs['search_keyword'], 
        limit=100
    )

# Define DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'tweet_pipeline',
    default_args=default_args,
    description='Tweet Extraction DAG',
    schedule_interval='*/15 * * * *',
    start_date=datetime(2024, 11, 20),
    catchup=False,
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_tweets',
        python_callable=run_extraction,
        op_kwargs={'search_keyword': '{keyword}t lang:en'}, # Change the keyword here
    )

    # Task: Trigger the Transform DAG after extraction
    trigger_transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform_dag',
        trigger_dag_id='transform_tweets',  # Transform DAG ID
        wait_for_completion=False,  # Set to True if you want to wait for the Transform DAG to finish
    )

    # Set dependencies: Extraction task triggers the Transform DAG
    extract_task >> trigger_transform_dag