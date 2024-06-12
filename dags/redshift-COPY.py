from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from plugins import slack

from datetime import datetime

'''
<Description>
dag_id : copy_tourism_to_redshift
task   : load_to_redshift

load_to_redshift - S3ToRedshiftOperator
S3 버킷에 있는 csv 파일을 Redshift 테이블에 REPLACE COPY
'''

# DAG 정의
default_args = {
    'owner': 'sangmin',
    'retries': 0,
    'on_failure_callback': slack.on_failure_callback
}

with DAG(
    dag_id = 'copy_tourism_to_redshift',
    start_date = datetime(2024,1,1),
    catchup=False,
    schedule_interval = '@once',
    default_args=default_args,
    tags=['API', 'COPY', 'Redshift'],
):
    
    load_to_redshift = S3ToRedshiftOperator(
        task_id='load_to_redshift',
        s3_bucket='dev-3-2-bucket',
        s3_key='locgoRegnVisitrDDList.csv',
        schema='wnsldjqja',
        table='tourism',
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

    load_to_redshift