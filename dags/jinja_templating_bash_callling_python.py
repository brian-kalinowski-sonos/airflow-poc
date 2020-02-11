from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, time



default_args={
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag=DAG(
    dag_id='jinja_template_usage_xcom',
    description="This dag serves to practice Jinja Templates and all use cases.",
    schedule_interval='@once',
    default_args=default_args
)

task1=BashOperator(
    task_id='task1',
    xcom_push=True,
    # do_xcom_push=True,
    bash_command='/Users/hardik.furia/opt/anaconda3/envs/airflow-poc/bin/python /Users/hardik.furia/PycharmProjects/airflow-poc/helper/hello_world.py',
    output_encoding='utf-8',
    dag=dag
)


task2=BashOperator(
    task_id='task2',
    bash_command='echo "{{ ti.xcom_pull("task1") }}"',
    output_encoding='utf-8',
    dag=dag
)

task1 >> task2
