# tourist_visitation_patterns_by_weather
We are developing data pipeline for the pattern of visiting tourist attractions according to weather

## 프로젝트 소개

- 주제
  - 기상 정보 (날씨, 강수 확률, 기온 등)을 바탕으로 각 관광지의 방문자 현황 파악
  
  - 기상 정보와 방문자 수의 상관 관계를 바탕으로 관광 날짜 선택을 위한 인사이트 제공
  
  - 업데이트 주기가 빠른 데이터를 활용하여 실시간 업데이트 제공

- 목적
  - Docker - Airflow 구축을 직접 경험하며 Image, Container, compose 등 Docker 개념 이해
  
  - Airflow를 활용한 데이터 파이프라인(DAG) 작성 및 관리 경험
  
  - DBT를 활용한 더 효과적으로 ELT를 수행하여 analytics 데이터 도출
  
## 프로젝트 구현
#### DB schema (DBT - Src Model)
![image](https://github.com/lv1turtle/tourist_visitation_patterns_by_weather/assets/32154881/f229c6c1-35a1-4304-9080-93639507bec8)

#### DB schema (DBT - Core Model)
![image](https://github.com/lv1turtle/tourist_visitation_patterns_by_weather/assets/32154881/555ff6ed-2c1f-4301-8b5e-5d7a1a82fc5a)

### SW Architecture
![image](https://github.com/lv1turtle/tourist_visitation_patterns_by_weather/assets/32154881/6c3c38a5-809d-4bb0-b047-be7ab43e0288)

### Airflow 서버 구축
- AWS EC2 내에서 Public하게 구축

- Dockerfile
  >1. Airflow 2.5.1 , Python3.8 버전을 사용하여 빌드
  >
  >2. root 사용자로 진입하여 airflow를 실행함에 있어서 필요한 패키지를 설치
  >
  >3. airflow(default) 계정으로 진입하여 dags폴더의 파일들을 airflow 내부의 dags폴더에 복사
  >
  >4. requirement.txt에 작성한 라이브러리들을 설치
  >
  >5. 마지막으로, airflow database를 초기화

- docker-compose.yaml 
  >1. Airlfow_Image만 가져오는 것이 아닌 Dockerfile을 사용하여 빌드
  >
  >2. Docker Container 내부 Airlfow의 Dag 실행 Server 시간을 한국 시간으로 교체
  >
  >3. 프로젝트 진행에 있어서 Example Dag는 필요 없기 때문에 False로 설정
  >
  >4. 이외에 interval, timeout, retries를 따로 설정

### ETL 프로세스

#### 관광 DAG - Tour API
- Data Source : https://www.data.go.kr/data/15101972/openapi.do#/
  
    ![image](https://github.com/lv1turtle/tourist_visitation_patterns_by_weather/assets/32154881/7c2bda9e-7412-47ce-97f8-4aeec0e0b2eb)

- **dag_id : tourism_data_pipeline**
    - S3에 저장된 csv 파일에 API의 데이터를 병합하여 S3에 저장 및 Redshift에 COPY
    - **Task**
        - **get_data_from_API** : API에서 해당 날짜의 데이터 추출
        - **get_csv_from_s3** : S3에 저장돼있는 csv 파일을 데이터 프레임 형태로 가져옴
        - **concat_data** : S3에 저장된 csv 파일과 API에서 추출한 데이터를 병합
        - **save_csv_to_s3** : 병합된 데이터를 S3에 저장
        - **load_to_redshift** : 업데이트된 데이터를 Redshift에 Full Refresh로 COPY

- **dag_id : tourism_data_only_COPY**
    - S3에 저장된 csv 파일을 Redshift에 COPY
    - csv에 저장은 완료했지만, COPY가 실패했을 경우를 위한 DAG
    - **Task**
        - **load_to_redshift** : S3에 저장된 csv 데이터를 Redshift에 Full Refresh로 COPY
        - (tourism_data_pipeline의 load_to_redshift와 동일)

#### 날씨 DAG - Weather API
- Data Source : https://www.data.go.kr/data/15056912/openapi.do#/
  
- **dag_id** : Weather_to_Redshift
    - API를 통해 가져온 data를 Redshift로 insert
    - Task
        - **get_data_by_api**:
        API에서 추출한 data를 pandas dataframe형태로 저장 → Redshift로 row 단위 insert
        
#### Slack 알림 서비스
- Airflow plugins를 활용한 Slack 알림 서비스 적용

    - **실패 알림** : DAG에 on_failure_callback=slack.on_failure_callback 설정
  ![image](https://github.com/lv1turtle/tourist_visitation_patterns_by_weather/assets/32154881/3c84ff6d-0014-497e-8f49-b84e7ef4a098)


    - **성공 알림** : 마지막 Task에 on_success_callback=slack.on_success_callback 설정

### ELT 프로세스

![image](https://github.com/lv1turtle/tourist_visitation_patterns_by_weather/assets/32154881/c976f66f-0722-4913-877a-fb30d3446dce)

- **RAW Data**
    - ETL 프로세스 및 dbt-seed를 통해  Redshift에 적재한 원시 데이터를 호출
- **Staging Model**
    - 원시 데이터를 가공하여 스테이징 모델을 정의
  
    - JOIN과 집계함수를 제외한 데이터 가공만을 수행
- **Core Model**
    - 스테이징 모델을 활용하여 분석에 적합한 모델을 정의
  
    - dim 모델은 데이터의 추가 및 수정이 거의 없는 컬럼들로 정의
  
    - fact 모델은 Incremental update를 수행
  
    - Agg 모델은 분석을 위한 양식에 맞춰 테이블 정의

### 데이터 시각화 - 대시보드 구성
#### 2021 ~ 2022
![image](https://github.com/lv1turtle/tourist_visitation_patterns_by_weather/assets/32154881/936ab573-48dc-4d7a-8c2d-3c4867efd5df)

#### 2024
![image](https://github.com/lv1turtle/tourist_visitation_patterns_by_weather/assets/32154881/96cdeb1e-36bf-441f-bf9b-a24185920c59)

#### 계절별
![image](https://github.com/lv1turtle/tourist_visitation_patterns_by_weather/assets/32154881/7905dd2d-ca57-493a-810f-29a4a1ad203b)

#### 날씨별
![image](https://github.com/lv1turtle/tourist_visitation_patterns_by_weather/assets/32154881/da010690-f08f-41e7-ae73-fdae2eeb1556)
