import requests
import pandas as pd

from airflow.hooks.S3_hook import S3Hook
from io import StringIO
from urllib.parse import urlencode


def get_csv_from_s3(**kwargs):
    try:
        print("Connect S3")
        hook = S3Hook(aws_conn_id='dev-3-2-bucket')
        key = kwargs["params"]["key"]
        bucket_name = kwargs["params"]["bucket_name"]
        print("Done")
    except Exception as e:
        error_message = f"dag_id : tourism_data_pipeline\ntask : get_csv_from_s3\nError : S3Hook Connection Error\nComment : {e}"
        raise Exception(error_message)

    try:
        print("Get csv file from S3")
        response = hook.read_key(key, bucket_name)
        df = pd.read_csv(StringIO(response))
        print("Done")
    except Exception as e:
        print("Create new csv file in s3 because it's not found")
        df = pd.DataFrame(columns=['signguCode', 'signguNm', 'daywkDivNm', 'touDivNm', 'touNum', 'baseYmd'])
        print("Done")
    
    try:
        kwargs['ti'].xcom_push(key='csv_data', value=df.to_json())
    except Exception as e:
        error_message = f"dag_id : tourism_data_pipeline\ntask : get_csv_from_s3\nError : task instance(ti).xcom_push Error\nComment : {e}"
        raise Exception(error_message)


def concat_data(**kwargs):
    try:
        print("Concat csv file in S3 with API Data")
        ti = kwargs['ti']
        df = pd.read_json(ti.xcom_pull(task_ids='get_csv_from_s3', key='csv_data'))
        data = pd.read_json(ti.xcom_pull(task_ids='get_data_from_API', key='api_data'))
        concat_df = pd.concat([df, data], ignore_index=True)
        print("Done")
    except Exception as e:
        error_message = f"dag_id : tourism_data_pipeline\ntask : concat_data\nError : Concat(S3 data, API data) Error\nComment : {e}"
        raise Exception(error_message)

    ti.xcom_push(key='concatenated_data', value=concat_df.to_json())
    