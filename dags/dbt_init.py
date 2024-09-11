from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import os
from datetime import datetime

# DAG 정의
default_args = {
    'owner': 'hyoungwon',
    # 이전 날짜의 task 인스턴스 중에서 동일한 task 인스턴스가 실패한 경우 실행되지 않고 대기.
    'depends_on_past': False,
}

with DAG(
    dag_id = 'dbt_init_once_seed_data',
    start_date = datetime(2024,6,2),
    # 밀린 task를 실행하지 않음
    catchup=False,
    # 한번만 실행
    schedule_interval = '@once',
    default_args=default_args,
):

    dbt_seed = BashOperator(
        task_id='load_seed_data_once',
        bash_command='cd /opt/airflow/dbt && dbt seed --profiles-dir .',
        env={
            **os.environ
        }
    )
    
    trigger_dbt_transform_analysis = TriggerDagRunOperator(
        task_id="trigger_dbt_transform_analysis",
        trigger_dag_id="transform_analysis_by_dbt",
        execution_date="{{ ds }}",
        reset_dag_run=True,
        wait_for_completion=True
    )

dbt_seed >> trigger_dbt_transform_analysis