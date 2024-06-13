from airflow.models import DagBag

def test_import_error():
    dag_bag = DagBag()
    assert len(dag_bag.import_error) == 0, "No Import Failures"