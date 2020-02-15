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
    dag_id="dynamic_dag", default_args=default_args, schedule_interval='@once'
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

def getUpstreamData(**kwargs):
    """
    Gets raw data from the kwargs['src_loc'] and stages the data to cwd. 
    returns stg_file_full_path, this return is an xcom push.
    """
    cwd=os.getcwd()
    shutil.copy2(kwargs['src_loc'],cwd)
    stg_file_full_path = cwd+'/'+ntpath.basename(kwargs['src_loc'])
    return stg_file_full_path
    
def dataTransformers(**kwargs):
    """
    Staged Data is transformed can be showcased using Branch Operator.
    """

    staged_data_df = pd.read_csv(kwargs['data_file'])
    staged_data_df = staged_data_df.iloc[:, (0,kwargs['arg']+1)]
    staged_data_df.to_csv(kwargs['data_file'],index=False)

def putDataDownstream(**kwargs):
    print('{} staged data will be moved to {} sink location\n'.format(kwargs['staged_file'],kwargs['sink_loc']))
    final_destination=kwargs['sink_loc']+ntpath.basename(kwargs['staged_file'])
    shutil.move(kwargs['staged_file'],final_destination)
    print('Sink File has been generated')
    
    
def createDynamicDag(task_id, callableFunction, args):
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
    data_transformers=config_file['data_transformers']
    data_sinks=config_file['data_sink']
    cwd=os.getcwd()

    for data_source in data_sources:
        for data_source,location in data_source.items():
            get_upstream_data = createDynamicDag('{}-getData'.format(data_source),
                                                 'getUpstreamData',
                                                 {
                                                     'src_loc': location,
                                                     'stg_loc': cwd
                                                 })
            staged_data_path=cwd+'/'+ntpath.basename(location)

            start >> get_upstream_data

                    get_upstream_data >> process_data

                    for data_sink in data_sinks:
                        for data_sink,location in data_sink.items():
                            put_data_downstream=createDynamicDag('{}-dataSink'.format(data_sink),
                                                                 'putDataDownstream',
                                                                 {'staged_file':staged_data_path,
                                                                  'sink_loc':location})

                            process_data >> put_data_downstream

                            put_data_downstream >> end
