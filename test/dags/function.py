import requests
import pandas as pd

from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
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
    
def get_data_from_API(**kwargs):
    df = pd.DataFrame(columns=['signguCode', 'signguNm', 'daywkDivCd', 'daywkDivNm', 'touDivCd', 'touDivNm', 'touNum', 'baseYmd'])

    url = kwargs["params"]["api_url"]
    row_number = kwargs["params"]["row_number"]
    start_ymd = kwargs["execution_date"].strftime('%Y%m%d')
    end_ymd = kwargs["execution_date"].strftime('%Y%m%d')
    page_number = 1

    try:
        while True:
            query_string = "?" + urlencode(
                {
                    "serviceKey": kwargs["params"]["service_key"],
                    "numOfRows": row_number,
                    "pageNo": page_number,
                    "MobileOS": "ETC",
                    "MobileApp": "TestApp",
                    "startYmd": start_ymd,
                    "endYmd": end_ymd,
                    "_type": "json"
                }
            )
            response = requests.get(url + query_string)
            items = response.json()['response']['body']['items']

            if len(items) == 0:
                if page_number == 1:
                    error_message = f"Tourism API does not have data in {start_ymd} ~ {end_ymd}\n"
                    raise Exception(error_message)
                break
            else:
                item = items['item']

            print(f"Scraping {page_number} page from API")
            page_df = pd.DataFrame(item)
            page_df['signguCode'] = page_df['signguCode'] + '00000'
            df = pd.concat([df, page_df], ignore_index=True)

            page_number += 1
            print("Done")
        df['touNum'] = df['touNum'].astype(float)
        df['touNum'] = df['touNum'].astype(int)
        df = df[['signguCode', 'signguNm', 'daywkDivNm', 'touDivNm', 'touNum', 'baseYmd']]
        print("Done Scraping")
    except Exception as e:
        error_message = f"dag_id : tourism_data_pipeline\ntask : get_data_from_API\nError : API Data Processing Error\nComment : {e}"
        raise Exception(error_message)

    kwargs['ti'].xcom_push(key='api_data', value=df.to_json())


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


def save_csv_to_s3(**kwargs):
    try:
        print("Connect S3")
        hook = S3Hook(aws_conn_id='dev-3-2-bucket')
        key = kwargs["params"]["key"]
        bucket_name = kwargs["params"]["bucket_name"]
        print("Done")
    except Exception as e:
        error_message = f"dag_id : tourism_data_pipeline\ntask : save_csv_to_s3\nError : S3Hook Connection Error\nComment : {e}"
        raise Exception(error_message)
    try:
        print("Save csv to s3")
        concatenated_data = pd.read_json(kwargs['ti'].xcom_pull(task_ids='concat_data', key='concatenated_data'))

        csv_buffer = StringIO()
        concatenated_data.to_csv(csv_buffer, index=False, encoding='utf-8')
        hook.load_string(csv_buffer.getvalue(), key, bucket_name, replace=True)
        print("Done")
    except Exception as e:
        error_message = f"dag_id : tourism_data_pipeline\ntask : save_csv_to_s3\nError : Saving csv file Error\nComment : {e}"
        raise Exception(error_message)


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
        error_message = f"dag_id : tourism_data_pipeline\ntask : load_to_redshift\nError : S3ToRedshiftOperator Error\nComment : {e}"
        raise Exception(error_message)