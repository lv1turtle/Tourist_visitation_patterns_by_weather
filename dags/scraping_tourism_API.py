from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from plugins import slack

import requests
from datetime import datetime
import pandas as pd
from io import StringIO
from urllib.parse import urlencode

'''
<Description>
dag_id : scraping_tourism_API
task   : [get_data_task, get_csv_task] >> concat_data_task >> save_csv_task

get_data_task - PythonOperator <get_csv_from_s3(**kwargs)>
S3 버킷 연결 및 "locgoRegnVisitrDDList.csv"(관광 데이터) 가져오기

get_csv_task - PythonOperator <get_data_from_API(**kwargs)>
관광 Open API를 호출 및 start_ymd ~ end_ymd 기간의 데이터를 가져오기

concat_data_task - PythonOperator <concat_data(**kwargs)>
S3에서 가져온 데이터와 API 호출로 가져온 데이터를 병합

save_csv_task - PythonOperator <save_csv_to_s3(**kwargs)>
병합한 데이터를 S3에 저장

load_to_redshift_task - PythonOperator <load_to_redshift(**kwargs)>
S3 버킷에 있는 csv 파일을 Redshift 테이블에 REPLACE COPY
'''


'''
<get_csv_from_s3(**kwargs)>

S3 버킷에 연결한 뒤 "locgoRegnVisitrDDList.csv"(관광 데이터) 가져온다.

에러 처리
1. S3 연결 실패 : 에러 문구 출력 후 DAG 종료 (raise)
2. 파일 찾기 실패 : 새로운 데이터 프레임 생성 (이후에 새로운 파일로 저장)
3. xcom_push 에러 : 에러 문구 출력 후 DAG 종료 (raise)
'''
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


'''
<get_data_from_API(**kwargs)>

관광 Open API를 호출하여 start_ymd ~ end_ymd 기간의 데이터를 가져온다.

에러 처리
1. API 호출 실패, 데이터 오류 등 : 에러 문구 출력 후 DAG 종료 (raise)
2. 해당 날짜에 API 데이터 X : 에러 문구 출력 후 DAG 종료 (raise)
'''
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


'''
<concat_data(**kwargs)>

S3에서 가져온 csv 데이터와 API 호출로 가져온 데이터를 병합한다.

에러 처리
1. 두 데이터가 모두 빈 값, 데이터 오류 등 : 에러 문구 출력 후 DAG 종료 (raise)
'''
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


'''
<save_csv_to_s3(**kwargs)>

병합한 데이터를 S3에 저장한다.

에러 처리
1. S3 연결 실패, 저장 실패 : 에러 문구 출력 후 DAG 종료 (raise)
'''
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
        error_message = f"dag_id : tourism_data_pipeline\ntask : load_to_redshift\nError : S3ToRedshiftOperator Error\nComment : {e}"
        raise Exception(error_message)


# DAG 정의
default_args = {
    'owner': 'sangmin',
    'retries': 0,
    'on_failure_callback': slack.on_failure_callback,
}

with DAG(
    dag_id = 'tourism_data_pipeline',
    start_date = datetime(2024,1,1),
    catchup=False,
    schedule_interval = '@daily',
    default_args=default_args,
    params={
        'bucket_name': 'dev-3-2-bucket',
        'key': 'locgoRegnVisitrDDList.csv'
    },
    tags=['API', 'Scraping', 'S3', 'Redshift', 'Tourism'],
):
    # Task 정의
    get_csv_task = PythonOperator(
        task_id='get_csv_from_s3',
        python_callable=get_csv_from_s3,
        provide_context=True,
    )

    get_data_task = PythonOperator(
        task_id='get_data_from_API',
        python_callable=get_data_from_API,
        provide_context=True,
        params = {
            'api_url': 'http://apis.data.go.kr/B551011/DataLabService/locgoRegnVisitrDDList',
            'row_number': 10000,
            'service_key': Variable.get("tourism_service_key")
        }
    )

    concat_data_task = PythonOperator(
        task_id='concat_data',
        python_callable=concat_data,
        provide_context=True,
    )

    save_csv_task = PythonOperator(
        task_id='save_csv_to_s3',
        python_callable=save_csv_to_s3,
        provide_context=True,
    )

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
    
    # 테이블이 변화되었으므로 dbt를 trigger
    trigger_dbt_transform_analysis = TriggerDagRunOperator(
        task_id="trigger_dbt_transform_analysis",
        trigger_dag_id="transform_analysis_by_dbt",
        execution_date="{{ ds }}",
        reset_dag_run=True,
        wait_for_completion=True
    )

    # Task 순서 정의
    [get_data_task, get_csv_task] >> concat_data_task >> save_csv_task >> load_to_redshift_task >> trigger_dbt_transform_analysis
