import pytest
from airflow.models import DagBag

dag_bag = DagBag(dag_folder='dags/', include_examples=False)
dag = dag_bag.get_dag('copy_tourism_to_redshift')

# dag_id로 dag를 Load
def test_dag_loaded():
    assert dag is not None
    assert dag.dag_id == 'copy_tourism_to_redshift'

# dag의 task 구성에 오타가 없는지 확인
def test_task_properties():
    task = dag.get_task('load_to_redshift')
    assert task.task_id == 'load_to_redshift'
    assert task.s3_bucket == 'dev-3-2-bucket'
    assert task.s3_key == 'locgoRegnVisitrDDList.csv'
    assert task.schema == 'wnsldjqja'
    assert task.table == 'tourism'
    assert task.copy_options == ['csv', 'IGNOREHEADER 1', "DATEFORMAT 'YYYYMMDD'", 'FILLRECORD']
    assert task.method == 'REPLACE'
    assert task.aws_conn_id == 'dev-3-2-bucket'
    assert task.redshift_conn_id == 'redshift_dev_db'
