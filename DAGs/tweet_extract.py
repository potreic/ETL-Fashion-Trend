from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from ETL import extract_tweet_all  # Import your extraction function

def run_extraction(**kwargs):
    search_keyword = kwargs['dag_run'].conf.get('search_keyword', 'default_keyword')
    extract_tweet_all(
        token='my-token',
        keyword=search_keyword,  # Use the dynamically provided keyword
        limit=100
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
    'tweet_extract',
    default_args=default_args,
    description='Tweet Extraction DAG',
    schedule_interval=None,  # This DAG is triggered on demand
    start_date=datetime(2024, 11, 20),
    catchup=False,
) as dag:

    # Task: Extract tweets
    extract_task = PythonOperator(
        task_id='extract_tweets',
        python_callable=run_extraction,
        provide_context=True,  # Ensure we can access `dag_run.conf`
    )

    # Task: Trigger the Transform DAG after extraction
    trigger_transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform_dag',
        trigger_dag_id='transform_tweets',  # Transform DAG ID
        wait_for_completion=False,  # Set to True if you want to wait for the Transform DAG to finish
        conf={'input_filename': '{{ dag_run.conf["search_keyword"] }}'}  # Pass keyword dynamically
    )

    # Set dependencies: Extraction task triggers the Transform DAG
    extract_task >> trigger_transform_dag
