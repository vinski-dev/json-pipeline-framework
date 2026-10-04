import os
from pathlib import Path

from airflow.models import DagBag


def test_no_import_errors():
    """Verify that all DAGs load without syntax or import errors."""
    project_root = Path(__file__).resolve().parent.parent
    dag_bag = DagBag(dag_folder=str(project_root / "dags"), include_examples=False)
    assert not dag_bag.import_errors, f"DAG import errors: {dag_bag.import_errors}"
