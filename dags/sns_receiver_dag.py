from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
import logging

from airflow.models import Variable


default_args = {
    'owner':'Obinna',
    'start_date':days_ago(1)
}

with DAG(
    dag_id = 'sns_receiver_dag',
    default_args = default_args
) as dag:
    @task
    def retrieve_config(**context):
        message = eval(context['dag_run'].conf['message'])

        logging.info(f"message: {message}")
        
    
    first_task = retrieve_config()
    
    