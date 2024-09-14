from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
import logging

default_args = {
    'owner':'Obinna',
    'start_date':days_ago(1)
}

with DAG(
    dag_id = 'read_sqs_and_process',
    default_args = default_args,
    schedule_interval = None
) as dag:
    read_queue = SqsSensor(
        task_id='poll_queue',
        sqs_queue='test_queue',
        max_messages=3,
        region_name='us-east-1',
        aws_conn_id='aws',
        mode='reschedule'
    )

    @task
    def extract_messages(**context):
        retrieved_messages = context['task_instance'].xcom_pull(task_ids='poll_queue',key='messages')
        # I want to parse message content only. Using eval() to auto convert to dictionary 
        retrieved_messages = [eval(message['Body']) for message in retrieved_messages]

        return retrieved_messages
    
    extract_msgs = extract_messages()

    @task
    def process_something(message):
        logging.info(f'Here the job can do something more with the message received:{message}')

    do_something_more = process_something.expand(message = extract_msgs)

    read_queue >> extract_msgs >> do_something_more
    
    

    