from airflow import DAG
from airflow.operators.bash import BashOperator

import os
from datetime import datetime

# DAG 정의
default_args = {
    'owner': 'hyoungwon',
    # 이전 날짜의 task 인스턴스 중에서 동일한 task 인스턴스가 실패한 경우 실행되지 않고 대기.
    'depends_on_past': False,
}

with DAG(
    dag_id = 'transform_analysis_by_dbt',
    start_date = datetime(2024,6,2),
    # 밀린 task를 실행하지 않음
    catchup=False,
    # 다른 DAG에 의해 Trigger 되게 설정하였기에 once로 설정
    schedule_interval = '@once',
    default_args=default_args,
):

    transform_analysis = BashOperator(
        task_id='transform_analaysis',
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir .',
        env={
            **os.environ
        }
    )

transform_analysis