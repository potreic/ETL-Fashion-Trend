from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from ETL import transform_pinterest  # Import the correct function

# Define parameters
regions = [{Tupple of List Region}]  
keywords = [{Tupple of List Keywords}]  

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'transform_pinterest',
    default_args=default_args,
    description='Transform Pinterest data to time series format',
    schedule_interval="*/2 * * * *",  # Run every 2 minutes
    start_date=datetime(2024, 11, 20, 8, 30),
    catchup=False,  # Prevent backfilling
) as dag:
    
    # Generate tasks dynamically for each region and keyword
    for region in regions:
        for keyword in keywords:
            transform_task = PythonOperator(
                task_id=f"transform_{keyword.replace(' ', '_')}_{region}",
                python_callable=transform_pinterest,  # Call your function here
                op_kwargs={
                    'region': region,  # Pass the region
                    'keyword': keyword,  # Pass the keyword
                    'input_dir': "{Directory for Input}",  # Specify input directory
                    'output_dir': "{Directory for Output}",  # Specify output directory
                },
            )
