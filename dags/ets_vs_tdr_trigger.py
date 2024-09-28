from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
import logging
from datetime import datetime, timedelta

default_args = {
    'owner':'Obinna',
    'start_date':days_ago(1),
    'trigger_rule':'all_done'
}

with DAG(
    dag_id = 'ets_vs_tdr_trigger',
    default_args = default_args,
    schedule_interval='08 02 * * *'
) as dag:
    @task
    def just_succeed():
        logging.info('I just want this task to be successful. Nothing much happening here')
        return True 
    
    successful_task = just_succeed()
    
    trigger_task = TriggerDagRunOperator(
        task_id='trigger_task_1',
        trigger_dag_id='ets_vs_tdr_receiver',
        trigger_run_id=f'from_trigger_dag_{datetime.now().strftime('%Y%m%d%H%M%S')}',
        conf = {
            'configs':
                [
                    {
                        'sample_param_1':'value_1'
                    },
                    {
                        'sample_param_2':'value_2'
                    }
                ]
            }
    )

    waiting_task = ExternalTaskSensor(
    task_id = "wait_sensor",
    external_dag_id="ets_vs_tdr_receiver",
    external_task_id = "two_min_task",
    mode="reschedule",
    execution_delta=timedelta(minutes=-2),
    timeout=60*60*23,
    retries = 10
    )

    @task
    def produce_params():
        # make dummy configs to return via Airflow xcom
        config_a = {'configs':[{'sample_param_1':'value_1'},{'sample_param_2':'value_2'}]}
        config_b = {'configs':[{'sample_param_3':'value_3'},{'sample_param_4':'value_4'}]}
        
        return [config_a, config_b]
    
    make_params = produce_params()

    multi_trigger_task = TriggerDagRunOperator.partial(
        task_id='trigger_task_2',
        trigger_dag_id='ets_vs_tdr_receiver',
        trigger_run_id=f'from_trigger_dag_{datetime.now().strftime('%Y%m%d%H%M%S%f')}'
    ).expand(conf=make_params)
    
    
    # set task dependencies
    successful_task >> trigger_task >> waiting_task >> make_params >> multi_trigger_task





    