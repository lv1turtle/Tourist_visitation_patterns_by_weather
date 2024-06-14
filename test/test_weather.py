import os
import requests
from airflow.models import Variable

def test_weather_get_data_from_API():
    service_key = Variable.get('weather_api_key')
    assert service_key is not None, "API Key does not exist in the environment variables."

    base_date, hours, course_id = '2024060212', '00', 1
    url = f"https://apis.data.go.kr/1360000/TourStnInfoService1/getTourStnVilageFcst1?serviceKey={service_key}&pageNo=1&numOfRows=1000000&dataType=JSON&CURRENT_DATE={base_date}&HOUR={hours}&COURSE_ID={course_id}"
    
    response = requests.get(url)
    assert response.status_code == 200, "API request Failed"
    
    data = response.json()
    items = data["response"]["body"]["items"]["item"]
    assert items is not None, "Error: The value retrieved from the API does not exist."
