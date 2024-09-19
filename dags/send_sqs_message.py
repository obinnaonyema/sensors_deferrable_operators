from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.sqs import SqsPublishOperator

import numpy as np
import logging

default_args = {
    'owner':'Obinna',
    'start_date':days_ago(1)
}

with DAG(
    dag_id = 'send_sqs_message',
    default_args = default_args,
    schedule_interval = None
) as dag:
    
    @task
    def create_messages():
        '''
        Returns a message in the format {"config_id":1,"run_type":"regular"}.
        It will randomly generating config ID and run_type
        '''
        run_types = ["regular","delta"]

        message_list = []
        # randomly generate 1 or more different messages
        for i in range(np.random.randint(1,10)):
            message_list.append(str({"config_id":np.random.randint(1,5), "run_type":np.random.choice(run_types,1)[0]}))
        
        logging.info("messages to be sent:", message_list)
        return message_list
    
    make_messages = create_messages()
    
    # I'm using dynamic tasks to simulate sending multiple messages
    publish_to_queue = SqsPublishOperator.partial(
        task_id='publish_to_queue',
        sqs_queue='test_queue',
        message_attributes=None,
        region_name='us-east-1',
        aws_conn_id='aws',
        delay_seconds=0,
    ).expand(message_content=make_messages)

    make_messages >> publish_to_queue

  
    

    