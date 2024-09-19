from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
import logging

from airflow.models import Variable


default_args = {
    'owner':'Obinna',
    'start_date':days_ago(1),
    'schedule_interval':None
}

with DAG(
    dag_id = 'sns_trigger_dag',
    default_args = default_args
) as dag:
    @task
    def some_task():
        logging.info('Do some magical work here')
        return True 
    
    first_task = some_task()

    publish_message = SnsPublishOperator(
    task_id="publish_message",
    target_arn=Variable.get("SNS_TOPIC_ARN"), # saved ARN as airflow variable
    message=str({"config_id":99,"run_type":"regular", "source":"sns_trigger_dag"}), #some interesting looking message
    aws_conn_id="aws",
    region="us-east-1"
    )
    
    first_task >> publish_message