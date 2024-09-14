from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
import logging

default_args = {
    'owner':'Obinna',
    'start_date':days_ago(1),
    'schedule_interval':None
}

with DAG(
    dag_id = 'generic_dag_a_upstream',
    default_args = default_args
) as dag:
    @task
    def just_succeed():
        logging.info('I just want this task to be successful. Nothing much happening here')
        return True 
    
    successful_task = just_succeed()

    