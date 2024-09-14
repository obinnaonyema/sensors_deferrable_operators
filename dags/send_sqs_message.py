from airflow import DAG, XComArg
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.sqs import SqsPublishOperator

import numpy as np
import logging

default_args = {
    'owner':'Obinna',
    'start_date':days_ago(1)
}

def create_message():
    '''
    Returns a message in the format {"config_id":1,"run_type":"regular"}.
    It will randomly generating config ID and run_type
    '''
    run_types = ["regular","delta"]

    return str({"config_id":np.random.randint(1,5), "run_type":np.random.choice(run_types,1)[0]})

with DAG(
    dag_id = 'send_sqs_message',
    default_args = default_args,
    schedule_interval = None
) as dag:
    # for the sake of simplicy, we'll replicate SqsPublishOperator because it doesn't support dynamic tasks right now.

    publish_to_queue = SqsPublishOperator(
        task_id='publish_to_queue',
        sqs_queue='test_queue',
        message_content=create_message(),
        message_attributes=None,
        region_name='us-east-1',
        aws_conn_id='aws',
        delay_seconds=0,
    )

    publish_again = SqsPublishOperator(
        task_id='publish_to_queue_again',
        sqs_queue='test_queue',
        message_content=create_message(),
        message_attributes=None,
        region_name='us-east-1',
        aws_conn_id='aws',
        delay_seconds=10,
    )

    publish_once_more = SqsPublishOperator(
        task_id='publish_to_queue_once_more',
        sqs_queue='test_queue',
        message_content=create_message(),
        message_attributes=None,
        region_name='us-east-1',
        aws_conn_id='aws',
        delay_seconds=10,
    )

    publish_to_queue >> publish_again >> publish_once_more
    
    

    