import os
import pytest
from airflow.models import DagBag

def test_no_import_errors():
    """Verify that all DAGs load without syntax or import errors."""
    dag_bag = DagBag(dag_folder=os.path.abspath('dags/'), include_examples=False)
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"