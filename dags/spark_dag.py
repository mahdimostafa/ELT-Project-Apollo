from datetime import datetime, timedelta
import os
import sys
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from  python_folder import load_script
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'load_job',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 29),
    'email': ['mahdi@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'priority_weight': 1#,
    #'end_date': datetime(2019, 6, 30)
}

dag = DAG('python_load_dag',
          default_args=default_args,
          description='Load data in parquet with Airflow',
          schedule_interval=None
        )


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


python_load = PythonOperator(dag=dag,
               task_id='my_task_powered_by_python',
               provide_context=False,
               python_callable=load_script.main
               )


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> python_load

python_load  >> end_operator
