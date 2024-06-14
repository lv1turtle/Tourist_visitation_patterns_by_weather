import pytest
import pandas as pd
import requests
import os

from airflow.models import DagBag, XCom
from airflow.utils import dates

from unittest.mock import patch, MagicMock, Mock
from function import get_csv_from_s3, concat_data

dag_bag = DagBag(dag_folder='/dags/', include_examples=False)
dag = dag_bag.get_dag('scraping_tourism_API')

# DAG 객체 확인
def test_dag_loaded():
    assert dag is not None
    assert dag.dag_id == 'scraping_tourism_API'
    
# Task명 일치여부
def test_tasks_exist():
    tasks = dag.tasks
    task_ids = [task.task_id for task in tasks]
    expected_tasks = ['get_csv_from_s3', 'get_data_from_API', 'concat_data', 'save_csv_to_s3']
    assert set(task_ids) == set(expected_tasks)
    
# Task의 의존성 테스트
def test_task_dependency():
    tasks = dag.tasks
    get_csv_task = [task for task in tasks if task.task_id == 'get_csv_from_s3'][0]
    get_data_task = [task for task in tasks if task.task_id == 'get_data_from_API'][0]
    concat_data_task = [task for task in tasks if task.task_id == 'concat_data'][0]
    save_csv_task = [task for task in tasks if task.task_id == 'save_csv_to_s3'][0]
    
    assert get_csv_task.downstream_task_ids == {'concat_data'}
    assert get_data_task.downstream_task_ids == {'concat_data'}
    assert concat_data_task.downstream_task_ids == {'save_csv_to_s3'}
    assert save_csv_task.downstream_task_ids == set()
    
# get_csv_from_s3 Function Logic Test
@patch('dags.scraping-API.S3Hook')
def test_get_csv_from_s3(mock_s3_hook):
    mock_response = MagicMock()
    csv_data = (
        "signguCode,signguNm,daywkDivNm,touDivNm,touNum,baseYmd",
        "1111000000,종로구,금요일,현지인(a),71358.00,20210101",
        "1111000000,종로구,금요일,외지인(b),122265.50,20210101",
        "1111000000,종로구,금요일,외국인(c),26.46,20210101",
        "1114000000,중구,금요일,현지인(a),58156.00,20210101",
        "1114000000,중구,금요일,외지인(b),107786.00,20210101"
    )
    mock_response.decode.return_value = csv_data
    mock_s3_hook.return_value.read_key_value = mock_response
    
    kwargs = {'params': {'bucket_name':'test-bucket', 'key':'test.csv'}, 'ti':MagicMock()}
    get_csv_from_s3(**kwargs)
    
    expected_df = pd.DataFrame({
        'signguCode':[1111000000, 1111000000, 1111000000, 1114000000, 1114000000],
        'signguNm':['종로구', '종로구', '종로구', '중구', '중구'],
        'daywkDivNm':['금요일', '금요일', '금요일', '금요일', '금요일'],
        'touDivNm':['현지인(a)', '외지인(b)','외국인(c)', '현지인(a)', '외지인(b)'],
        'touNum':[71358.00, 122265.50, 26.46, 58156.00, 107786.00],
        'baseYmd':[20210101, 20210101, 20210101, 20210101, 20210101]
    })
    
    xcom_data = XCom.get_one(key='csv_data', task_id='get_csv_from_s3')
    result_df= pd.read_json(xcom_data)
    # 결과 Dataframe과 예상 Dataframe 비교
    assert result_df.equals(expected_df)
    
# API Load Test
def test_tourism_get_data_from_API():
    service_key = os.getenv('AIRFLOW_TOURISM_API_KEY')
    assert service_key is not None,"API Key does not exist in the environment variables."
    
    url = "http://apis.data.go.kr/B551011/DataLabService/locgoRegnVisitrDDList"
    params = {
        "serviceKey": service_key,
        "numOfRows": 1,
        "pageNo": 1,
        "MobileOS": "ETC",
        "MobileApp": "TestApp",
        "startYmd": 20240101,
        "endYmd": 20240101,
        "_type": "json"
    }
    
    response = requests.get(url, params=params)
    
    assert response.status_code == 200, "API request Failed"
    
    data = response.json()
    assert 'response' in data, "Response Error"
    assert 'body' in data['response'], "Body Error"
    assert 'items' in data['response']['body'], "Item Error"

# s3에서 가져온값과 API로 받아온 값을 빈 값으로 하여
# 고의로 에러를 발생시킨다.
def test_concat_data():
    with patch('airflow.models.xcom.TaskInstance.xcom_pull') as mock_xcom_pull:
        mock_xcom_pull.side_effect = [
            Mock(return_value='[]'),
            Mock(return_value='[]')
        ]
        
        try:
            concat_data(ti=Mock())
        except Exception as e:
            expected_error_message = "dag_id : tourism_data_pipeline\ntask : concat_data\nError : Concat(S3 data, API data) Error\nComment : Data load issue from S3 and API"
            assert str(e) == expected_error_message, f"Expected error message: {expected_error_message}, but : {str(e)}"
        
        else:
            raise AssertionError("Expected an exception to be raised, but none was raised.")
