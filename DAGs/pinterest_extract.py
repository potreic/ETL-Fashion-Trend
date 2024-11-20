from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from ETL import extract_pinterest  

# Define parameters
regions = [{Tupple of Region in ISO standard}]
keywords = [{Tupple of Keywords}]
access_token = "{REDACTED}"  # Replace with your actual token

# Total combinations of regions and keywords
total_combinations = len(regions) * len(keywords)

DAG_START_TIME = datetime(2024, 11, 20, 7, 15)   # Set to Year, Month, Date, Hour, Minute

# Function to determine the current parameters for the run
def get_params_for_iteration(iteration):
    region = regions[iteration // len(keywords)]  # Determine the region
    keyword = keywords[iteration % len(keywords)]  # Determine the keyword
    return region, keyword

# Wrapper function to execute extract_pinterest
def run_extraction(**kwargs):
    # Calculate the current iteration index
    execution_date = kwargs['execution_date']
    
    # Convert execution_date to offset-naive for comparison
    execution_date_naive = execution_date.replace(tzinfo=None)

    # Calculate the iteration index
    iteration = int((execution_date_naive - DAG_START_TIME).total_seconds() // (5 * 60))  # Iteration based on 5-minute intervals

    # Stop condition: if iteration exceeds total combinations, stop
    if iteration >= total_combinations:
        print("All regions and keywords have been processed. Stopping.")
        return

    # Get parameters for the current iteration
    region, keyword = get_params_for_iteration(iteration)
    print(f"Running extraction for region={region}, keyword={keyword}")
    
    # Run the extraction function
    extract_pinterest(
        regions=[region],
        keywords=(keyword,),
        access_token=access_token
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
    'pinterest_trends_extraction',
    default_args=default_args,
    description='Extract Pinterest trends for regions and keywords',
    schedule_interval="*/5 * * * *",  # Run every 5 minutes
    start_date=DAG_START_TIME,
    catchup=False,
) as dag:
    
    extract_task = PythonOperator(
        task_id='Extract_Pinterest',
        python_callable=run_extraction,
        provide_context=True,  # Provide context to access execution_date
    )
