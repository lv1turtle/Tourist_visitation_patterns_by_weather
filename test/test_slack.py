import os
import pytest
import requests
from unittest.mock import patch
from airflow.models import Variable

def send_message_to_a_slack_channel(message):
    # url = "https://slack.com/api/chat.postMessage"
    slack_url = "https://hooks.slack.com/services/" + Variable.get("slack_url")
    headers = {
        'content-type': 'application/json',
    }
    data = {"username": "sangmin", "text": message}
    r = requests.post(slack_url, json=data, headers=headers)
    return r

# pytest.fixure를 사용하면 테스트 실행 전후에 특정 작업을 넣어줄 수 있다.
@pytest.fixture
def test_slack_webhook_url(monkeypatch):
    # pytest의 monkeypatch plugin을 사용하면 환경변수를 일시적으로 변경해줄 수 있다.
    slack_url = "https://hooks.slack.com/services/" + Variable.get("slack_url")
    
    if not slack_url:
        pytest.skip("Black ERROR : Slack_Url does not exist in the environment variables.")
    
    
    monkeypatch.setenv("AIRFLOW_SLACK_URL", slack_url)
    
    yield
    monkeypatch.undo()
    
@patch('airflow.models.Variable.get')
def test_send_message(mock_variable_get, test_slack_webhook_url):
    slack_url = "https://hooks.slack.com/services/" + Variable.get("slack_url")
    
    # mock_variable_get.return_value = slack_url
    
    test_message = "Send a test message from airflow"
    
    with patch('requests.post') as mock_post:
        # Mock으로 설정한 URL과 테스트 메시지 전달한다.
        response = send_message_to_a_slack_channel(test_message)
        
        # requests.post 함수에 올바른 URL과 데이터가 전달되었는지 확인한다.
        # mock_post.assert_called_once_with(slack_url, json={"text": test_message})

        # response 객체의 status_code가 200인지 확인한다.
        assert response.status_code == 200, "Failed to send message to Slack."
