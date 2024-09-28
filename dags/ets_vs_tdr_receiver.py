from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
import logging
import time

default_args = {
    'owner':'Obinna',
    'start_date':days_ago(1)
}

with DAG(
    dag_id = 'ets_vs_tdr_receiver',
    default_args = default_args,
    schedule_interval='10 02 * * *'
) as dag:
    @task
    def first_task(**context):
        try:
            # check if DAG was started by config
            param = context['dag_run'].conf['configs']
            logging.info(f'This DAG was started with config: {param}')
            # you can do something more here or pass to downstream task

        except KeyError:
            logging.info('No config received. Will run DAG with regular flow')
    
    task_1 = first_task()

    @task
    def two_min_task(**context):
        logging.info('Waiting for 2 minutes...')
        time.sleep(120)
        logging.info('Task completed!')

    delay_task = two_min_task()

    task_1 >> delay_task

    