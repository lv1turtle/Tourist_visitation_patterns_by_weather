from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import requests
import logging
from plugins import slack

"""
dag_id: Weather_to_Redshift

task_id: get_data_by_api
api에서 바로 redshift에 적재

스케쥴: @daily
"""

def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id="redshift_dev_db")
    return hook.get_conn().cursor()


@task
def get_data_by_api(schema, table, api_key, current_date):

    df = pd.DataFrame(
        columns=[
            "tm",
            "thema",
            "courseAreaId",
            "spotName",
            "spotAreaName",
            "th3",
            "ws",
            "sky",
            "rhm",
            "pop",
        ]
    )

    current_date = datetime.strptime(current_date, "%Y-%m-%d")

    base_date = (current_date - timedelta(days=1)).strftime(
        "%Y%m%d12"
    )  # 하루 전날 기준
    hours = "00"

    # 전 관광지를 조회
    for idx in range(1, 438 + 1):
        try:
            course_id = str(idx)
            url = f"https://apis.data.go.kr/1360000/TourStnInfoService1/getTourStnVilageFcst1?serviceKey={api_key}&pageNo=1&numOfRows=1000000&dataType=JSON&CURRENT_DATE={base_date}&HOUR={hours}&COURSE_ID={course_id}"
            response = requests.get(url)
            response.raise_for_status()  # Check if the request was successful
            data = response.json()
            items = data["response"]["body"]["items"]["item"]
            if items:
                item = items[
                    -1
                ]  # Select the last item (representative row for the hour)
                df = pd.concat([df, pd.DataFrame([item])], ignore_index=True)
        except Exception as e:
            logging.error(f"Empty course_id: {course_id}: {e}")
            continue

    ret = [
        f"('{row['tm']}', '{row['thema']}', '{row['courseAreaId']}', '{row['spotName']}', '{row['spotAreaName']}', {row['th3']}, {row['ws']}, {row['sky']}, {row['rhm']}, {row['pop']})"
        for _, row in df.iterrows()
    ]

    cur = get_Redshift_connection()
    insert_sql = f"INSERT INTO {schema}.{table} VALUES " + ", ".join(ret)
    logging.info(insert_sql)

    try:
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")

        error_message = f"dag_id : Weather_to_Redshift\ntask : get_data_by_api\nError : weather API Insert Error\nComment : {e}"
        raise Exception(error_message)


# DAG 파라미터 재 설정 필요
with DAG(
    dag_id="Weather_to_Redshift",
    start_date=datetime(2024, 6, 2),  # 날짜가 미래인 경우 실행이 안됨
    schedule_interval="@daily",  # 적당히 조절 -> 매일 실행하도록 설정
    max_active_runs=1,
    catchup=False,
    default_args={
        "owner": 'heejong',
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
        "on_failure_callback": slack.on_failure_callback,
    },
) as dag:

    get_data = get_data_by_api(
        schema="wnsldjqja",
        table="weather_info",
        api_key=Variable.get("weather_api_key"),
        current_date="{{ ds }}",
    )
