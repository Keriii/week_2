import sys
import os
import pandas as pd

from sqlalchemy import create_engine
from datetime import timedelta,datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append(os.path.abspath(".."))


from scripts.transform_df import transform_df
from scripts.load_pg import load_data


# Define the default dag arguments.
default_args = {
    'owner': 'Kerod',
    'depends_on_past': False,
    'email': ['kerod53@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

# Define the dag, the start date and how frequently it runs.
# I chose the dag to run everday by using 1440 minutes.
dag = DAG(
    dag_id='ETLDag',
    default_args=default_args,
    start_date=datetime(2023, 12, 22),
    schedule_interval= '@daily')#timedelta(minutes=1440)) 


# First task is to query get the weather from openweathermap.org.
task1 = PythonOperator(
    task_id='Extract_and_transform',
    provide_context=True,
    python_callable=transform_df,
    dag=dag)


# Second task is to transform the data
task2 = PythonOperator(
    task_id='load_to_pg',
    provide_context=True,
    python_callable=load_data,
    dag=dag)

# Set task1 "upstream" of task2
# task1 must be completed before task2 can be started
task1 >> task2