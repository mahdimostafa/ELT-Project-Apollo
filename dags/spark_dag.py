from datetime import datetime, timedelta
import os
import sys
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from operators.data_quality import PostgresOperator
from airflow.operators.bash_operator import BashOperator
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

run_quality_checks = PostgresOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    retry_times=1,
    # database=["postgres"],
    tables=["new"],
    sql="""select count(*) from public.new """
    # custom_check={
    #     'test'      : 'SELECT COUNT(*) FROM "new"'
    #     # 'expected'  : 28
    # }
)

dbt_view = BashOperator(
        task_id='dbt_view_load'
        ,bash_command="cd /Users/mahdimostafa/airflow/dags/dbt_project && dbt run"
        ,params = {'DATE' : 'this-should-be-a-date'}
        ,dag=dag
    )
#possibly create sub dags dbt to create another dbt i.e.  dbt1 >> dbt3 / dbt2 >> dbt3  / dbt1 >> dbt2 /

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> python_load
python_load  >> run_quality_checks >> dbt_view
dbt_view >> end_operator
