import os
import pytest
from unittest.mock import patch
from airflow.models import Variable
from dags.plugins.slack import send_message_to_a_slack_channel

# pytest.fixure를 사용하면 테스트 실행 전후에 특정 작업을 넣어줄 수 있다.
@pytest.fixture
def test_slack_webhook_url(monkeypatch):
    # pytest의 monkeypatch plugin을 사용하면 환경변수를 일시적으로 변경해줄 수 있다.
    slack_url = os.environ.get("AIRFLOW_SLACK_URL")
    
    if not slack_url:
        pytest.skip("Black ERROR : Slack_Url does not exist in the environment variables.")
    
    
    monkeypatch.setenv("AIRFLOW_SLACK_URL", slack_url)
    
    yield
    monkeypatch.undo()
    
@patch('airflow.models.Variable.get')
def test_send_message(mock_variable_get, test_slack_webhook_url):
    slack_url = os.environ.get("AIRFLOW_SLACK_URL")
    
    mock_variable_get.return_value = slack_url
    test_message = "Send a test message from airflow"
    
    response = send_message_to_a_slack_channel(test_message)
    
    assert response.status_code == 200, "Failed to send message to Slack."
