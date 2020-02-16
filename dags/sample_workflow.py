import yaml
from airflow import DAG
from datetime import datetime, timedelta, time
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import shutil
import os
import ntpath
import pandas as pd

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id="sample_workflow", default_args=default_args, schedule_interval='@once'
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

def captureData(**kwargs):
    """
    Gets raw data from the kwargs['src_loc'] and stages the data to cwd. 
    returns stg_raw_path, this return is an xcom push.
    """
    cwd=os.getcwd()
    shutil.copy2(kwargs['src_loc'],cwd)
    stg_rawFile_path = cwd+'/'+ntpath.basename(kwargs['src_loc'])
    return stg_rawFile_path
    
def cleanseData(**kwargs):
    """
    Converts stg-raw-data to json and stores it back in the staging location.
    Preserving the staged-raw-data.
    returns the
    """
    stg_rawFile_path=kwargs['task_instance'].xcom_pull(kwargs['task_id'])
    stg_raw_df = pd.read_csv(stg_rawFile_path)
    stg_cleanseFile_path=stg_rawFile_path[:-4]+'.json'
    stg_raw_df.to_json(stg_cleanseFile_path,orient='table')
    return stg_cleanseFile_path

def sinkData(**kwargs):
    stg_cleanseFile_path=kwargs['task_instance'].xcom_pull(kwargs['task_id'])
    shutil.copy2(stg_cleanseFile_path,kwargs['data_sink'])

    
    
def createTask(task_id, callableFunction, args):
    task = PythonOperator(
        task_id=task_id,
        provide_context=True,
        do_xcom_push=True,
        python_callable=eval(callableFunction),
        op_kwargs=args,
        dag=dag
    )
    return task

end = DummyOperator(
    task_id='end',
    dag=dag)

with open('/Users/hardik.furia/PycharmProjects/airflow-poc/yml/generated-yaml.yaml') as f:
    config_file=yaml.safe_load(f)
    data_sources=config_file['data_sources']
    data_sink=config_file['data_sink']


    for data_source in data_sources:
        for data_source,location in data_source.items():
            capture_data = createTask('{}-captureData'.format(data_source),\
                                      'captureData',\
                                      {
                                          'src_loc': location,
                                      })
            start >> capture_data

            cleanse_data = createTask('{}-cleanseData'.format(data_source),\
                                      'cleanseData',\
                                      {
                                          'task_id' : '{}-captureData'.format(data_source)
                                      })
            capture_data >> cleanse_data

            sink_data = createTask('{}-sinkData'.format(data_source),\
                                    'sinkData',
                                    {
                                       'task_id' : '{}-cleanseData'.format(data_source),
                                        'data_sink' : data_sink
                                   })

            cleanse_data >> sink_data
            sink_data >> end

