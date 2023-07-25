# Data Engineer Airflow Project From Postgres to AWS S3

![airflow](https://github.com/askintamanli/Data-Engineer-Airflow-Project-From-Postgres-to-AWS-S3/assets/63555029/3d3b27c5-0620-490b-84d1-85031372d515)


### Hello everyone. There is not too much case-study project about data engineer field. This is really good project to understand what doese data engineer do in real life.
### Today we're gonna get data from Postgres database and upload the data to AWS S3 bucket. Airflow is gonna upload the data automatically to S3 bucket daily. (I run Airflow in Docker. If you don't know how to run Airflow in Docker, you can read my previously article.)

## Requirements
- Basic knowlage Airflow
- Basic knowlage Docker
- Intermediate level Python
- Basic knowlage SQL
- Basic knowlage AWS (S3)
- Basic knowlage Postgres

## To do list step by step
1. [Create a Postgres database and table. Load data to table.](#)
2. [Create AWS S3 bucket.](#)
3. [Set Airflow connections for AWS and Postgres.](#)
4. [Create Airlfow DAG and Task.](#)
5. [Run the DAG.](#)

## 1.1. Create Postgres database
Open the pgAdmin page and create a new database named 'CRM'.
![1](https://github.com/askintamanli/Data-Engineer-Airflow-Project-From-Postgres-to-AWS-S3/assets/63555029/799ff888-943c-47ef-a0c4-818aafd81f29)

## 1.2. Create table named 'orders'
Go to Tables and open Query Tool. We're gonna write SQL statement for create 'orders' table.
```
create table if not exists public.orders (
 order_id character varying,
 date date,
 product_name character varying,
 quantity integer,
 primary key (order_id)
)
```

## 1.3. Load data to table
Right click to 'orders' table and select 'Import/Export data'. You can get the data from my github repository.

![2](https://github.com/askintamanli/Data-Engineer-Airflow-Project-From-Postgres-to-AWS-S3/assets/63555029/7fa3f28c-58bb-4a78-8052-f87d59803272)
![3](https://github.com/askintamanli/Data-Engineer-Airflow-Project-From-Postgres-to-AWS-S3/assets/63555029/44f68fa3-fea7-4fa4-abae-64f941c88a80)

Than click OK button. Now let's check our data. Open again Query Tool page and write this SQL statement. Make sure that run only this statement.

```
SELECT * FROM public.orders
```
![4](https://github.com/askintamanli/Data-Engineer-Airflow-Project-From-Postgres-to-AWS-S3/assets/63555029/e12c439c-a2b0-44d0-847c-c72709dda22d)

## 2. Create AWS S3 Bucket
Go to AWS S3 page and create a bucket. My bucket name is 'from-postgres-data'.

![5](https://github.com/askintamanli/Data-Engineer-Airflow-Project-From-Postgres-to-AWS-S3/assets/63555029/89823331-b97a-40f7-9225-90dd53ba2c80)

## 3.1. Create Postgres connection
We should create connections before create Airflow DAG.

Open the Airflow webserver → Admin → Connections →Create new connection

- Connection Id: postgres_localhost (you can change it)
- Connection Type: Postgres
- Host: postgres
- Schema: CRM (this space must be same with postgres database name)
- Login: airflow (you can learn it from docker-compose.yaml file)
- Password: airflow (you can learn it from docker-compose.yaml file)
- Port: 5432 (you can learn port number from pgAdmin page)

 ![6](https://github.com/askintamanli/Data-Engineer-Airflow-Project-From-Postgres-to-AWS-S3/assets/63555029/67640553-1934-4189-a463-587c7c35b419)

 Test it, if there is no problem save it.

 ## 3.2. Create AWS S3 conneciton
 Open the Airflow webserver → Admin → Connections →Create new connection

- Connection Id: s3_connection (you can change it)
- Connection Type: Amazon Web Services
- AWS Access Key ID: (You can learn it form AWS)
- AWS Secret Access Key ID: (You can learn it form AWS)

![7](https://github.com/askintamanli/Data-Engineer-Airflow-Project-From-Postgres-to-AWS-S3/assets/63555029/d5908aa4-5431-4f17-bcbb-e1f73a2dfcb8)

Test it, if there is no problem save it.

## 4. Create Airflow DAG and Task
This is the python file for DAG and Task. There is only 1 task in this sample. I want to make this esay to understand.

```
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
```

## 5.1. Run the DAG
Open and login Airflow webserver. Than run the DAG.

![8](https://github.com/askintamanli/Data-Engineer-Airflow-Project-From-Postgres-to-AWS-S3/assets/63555029/3907ea4a-09b6-4791-ad6b-8e42bc917ccf)

ALL SUCCESS. Let's check the S3 bucket.

![9](https://github.com/askintamanli/Data-Engineer-Airflow-Project-From-Postgres-to-AWS-S3/assets/63555029/f6328ef8-ad9f-42a9-a88d-bcb4ad169ced)

Today is July 25 and we set the start date July 20 in python file. So we have 20–24 July data. Tomorrow Airflow is going to upload data of 25 July automatically for us.

## That's it. This is the end of this project. Thank you for your interest.










