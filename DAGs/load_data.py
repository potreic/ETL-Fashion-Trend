from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from datetime import datetime

# Function to load CSV data into PostgreSQL
def load_csv_directly_to_postgres(ds, **kwargs):
    try:
        # Extract parameters from dag_run.conf
        table_name = kwargs['dag_run'].conf.get('table_name')
        csv_file_path = kwargs['dag_run'].conf.get('csv_file_path')

        if not table_name or not csv_file_path:
            raise ValueError("Both 'table_name' and 'csv_file_path' must be provided in dag_run.conf.")

        # Load CSV into a DataFrame
        df = pd.read_csv(csv_file_path)

        # Connect to PostgreSQL using PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id='postgres_fashion')
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()

        # Create table dynamically based on DataFrame structure
        columns = ", ".join([f"{col} TEXT" for col in df.columns])  # Assuming all columns are TEXT for simplicity
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
        cursor.execute(create_table_query)

        # Insert data row-by-row
        for _, row in df.iterrows():
            values = ", ".join([f"'{str(v).replace('\'', '\'\'')}'" for v in row])  # Escape single quotes
            insert_query = f"INSERT INTO {table_name} VALUES ({values});"
            cursor.execute(insert_query)

        # Commit and close
        connection.commit()
        cursor.close()
        connection.close()

        print(f"Data successfully loaded into table: {table_name}")

    except Exception as e:
        print(f"Error during data load: {e}")

# Default args for DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'start_date': datetime(2024, 11, 20),
}

# Define DAG
with DAG(
    'load_csv_to_postgres_direct',
    default_args=default_args,
    description='Load CSV to PostgreSQL using direct SQL',
    schedule_interval=None,
    catchup=False,
) as dag:

    load_csv_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_directly_to_postgres,
        provide_context=True,
    )

    load_csv_task
