from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from ETL import load_tweets, load_json, integrate_data  # Import your functions

# Airflow DAG definition
default_args = {
    'start_date': datetime(2024, 11, 20),
    'retries': 1,
}

with DAG(
    dag_id="data_integration_pipeline",
    default_args=default_args,
    schedule_interval=None,  # This DAG will run on demand
    catchup=False,
    tags=["integration", "data_pipeline"],
) as dag:

    # Task to load tweets data
    def load_tweets_task(**kwargs):
        filename = kwargs['dag_run'].conf.get('tweet_filename', 'default_tweet_file')
        return load_tweets(filename)

    # Task to load Pinterest data
    def load_json_task(**kwargs):
        filename = kwargs['dag_run'].conf.get('pinterest_filename', 'default_pinterest_file')
        return load_json(filename)

    # Task to integrate data
    def integrate_data_task(**kwargs):
        tweet_filename = kwargs['dag_run'].conf.get('tweet_filename', 'default_tweet_file')
        pinterest_filename = kwargs['dag_run'].conf.get('pinterest_filename', 'default_pinterest_file')
        output_filename = kwargs['dag_run'].conf.get('output_filename', 'merged_data')

        # Load the data using the imported functions
        df_tweets = load_tweets(tweet_filename)
        df_pinterest = load_json(pinterest_filename)

        # Integrate the data
        integrate_data(df_tweets, df_pinterest, output_filename)

    # Define the tasks
    load_tweets_op = PythonOperator(
        task_id="load_tweets",
        python_callable=load_tweets_task,
        provide_context=True,
    )

    load_json_op = PythonOperator(
        task_id="load_json",
        python_callable=load_json_task,
        provide_context=True,
    )

    integrate_data_op = PythonOperator(
        task_id="integrate_data",
        python_callable=integrate_data_task,
        provide_context=True,
    )

    # Define task dependencies
    [load_tweets_op, load_json_op] >> integrate_data_op