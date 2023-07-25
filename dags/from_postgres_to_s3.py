from datetime import datetime,timedelta
import csv
import logging
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
from tempfile import NamedTemporaryFile

#Default arguments
default_args = {
    'owner':'askin_owner',
    'retries':5,
    'retry_Delay':timedelta(minutes=10)
}

#Main function
# Meaning of 'ds_nodash' is today's execution
# Meaning of 'next_ds_nodash' is next execution
def postgres_to_csv(ds_nodash , next_ds_nodash): 

    # Postgres connection
    hook = PostgresHook(postgres_conn_id = "postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()
    # Get the data between today and next execution
    cursor.execute("select * from orders where date >= %s and date < %s",
                   (ds_nodash , next_ds_nodash))
    
    # Create temporary file and upload data to S3 bucket.
    with NamedTemporaryFile(mode="w",suffix=f"{ds_nodash}") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
        f.flush()
        cursor.close()
        conn.close()

        # S3 connection
        s3_hook = S3Hook(aws_conn_id = "s3_connection")
        s3_hook.load_file(
            filename = f.name,
            key =  f"orders/{ds_nodash}.txt",
            bucket_name = "from-postgres-data",
            replace = True
        )
        logging.info("Orders file {ds_nodash} has been pushed to S3", f.name)

# DAG
with DAG(
    dag_id = "from_postgres_to_s3_v01", # Dag name
    default_args=default_args, # Default Arguments
    start_date=datetime(2023,7,20), # Start it from this date
    schedule_interval="@daily" # Execute the task daily
)as dag:
    # TASK 
    task1 = PythonOperator( # Task created by PythonOperator
        task_id = "from_postgres_to_s3", # Task name
        python_callable=postgres_to_csv # Task function
    )
    task1