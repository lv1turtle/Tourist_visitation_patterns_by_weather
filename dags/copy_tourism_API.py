from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from plugins import slack

from datetime import datetime

'''
<load_to_redshift(**kwargs)>
S3 버킷에 있는 csv 파일을 Redshift 테이블에 REPLACE COPY한다.

에러 처리
1. S3ToRedshiftOperator 에러 : 에러 문구 출력 후 DAG 종료 (raise)
'''
def load_to_redshift(**kwargs):
    s3_bucket = kwargs["params"]["bucket_name"]
    s3_key = kwargs["params"]["key"]
    schema = kwargs["params"]["schema"]
    table = kwargs["params"]["table"]

    try:
        load_to_redshift = S3ToRedshiftOperator(
            task_id='load_to_redshift',
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            schema=schema,
            table=table,
            copy_options=[
                'csv',
                'IGNOREHEADER 1',
                "DATEFORMAT 'YYYYMMDD'",
                'FILLRECORD'
            ],
            method='REPLACE',
            aws_conn_id='dev-3-2-bucket',
            redshift_conn_id='redshift_dev_db'
        )
        load_to_redshift.execute(context=kwargs)
    except Exception as e:
        error_message = f"dag_id : tourism_data_only_COPY\ntask : load_to_redshift\nError : S3ToRedshiftOperator Error\nComment : {e}"
        raise Exception(error_message)


# DAG 정의
default_args = {
    'owner': 'sangmin',
    'retries': 0,
    'on_failure_callback': slack.on_failure_callback,
}

with DAG(
    dag_id = 'tourism_data_only_COPY',
    start_date = datetime(2024,1,1),
    catchup=False,
    schedule_interval = '@once',
    default_args=default_args,
    params={
        'bucket_name': 'dev-3-2-bucket',
        'key': 'locgoRegnVisitrDDList.csv'
    },
    tags=['API', 'COPY', 'S3', 'Redshift', 'Tourism'],
):

    load_to_redshift_task = PythonOperator(
        task_id='load_to_redshift',
        python_callable=load_to_redshift,
        provide_context=True,
        params = {
            'schema': 'wnsldjqja',
            'table': 'tourism',
        },
        on_success_callback=slack.on_success_callback,
    )

    # Task 순서 정의
    load_to_redshift_task
