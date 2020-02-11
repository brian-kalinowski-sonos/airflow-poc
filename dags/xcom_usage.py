import yaml
from airflow import DAG
from datetime import datetime, timedelta, time
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
import shutil
import os
import ntpath
import pandas as pd



DATA_SOURCE_LOC='/Users/hardik.furia/2016_january'
DATA_STAGING_LOC='/Users/hardik.furia/PycharmProjects/airflow-poc/staged_data'
DATA_SINK_LOC='/Users/hardik.furia/data_sink'


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 6),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

dag = DAG(
    dag_id="x_com_usage_demo",
    default_args=default_args,
    schedule_interval='@once'
)


def task1():
    print("This is an example workflow to understand how xcom works.\n")
    print("We return a data value which was generated in this task1.\n")
    value=10
    print(value," was returned from task1")
    return value

def task2(**context):
    print("This is task2, here we'll pull the value which was pushed in task1.\n")
    value = context['task_instance'].xcom_pull(task_ids=['task1','task3'])
    print(value," was received by the task 2")
    # value=value+5

def task3():
    value=5
    return value

task1=PythonOperator(
    task_id='task1',
    python_callable=task1,
    # provide_context=True,
    do_xcom_push=True,
    dag=dag
)



task2=PythonOperator(
    task_id='task2',
    python_callable=task2,
    provide_context=True,
    do_xcom_push=True,
    dag=dag
)

task3=PythonOperator(
    task_id='task3',
    python_callable=task3,
    # provide_context=True,
    do_xcom_push=True,
    dag=dag
)



task1 >> task2
task3 >> task2








