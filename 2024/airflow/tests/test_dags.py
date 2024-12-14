from datetime import datetime, timezone
from airflow.models import DagBag, BaseOperator
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType
import pytest


@pytest.fixture(scope="session")
def dag_bag():
	return DagBag(include_examples=False)


def get_task(dag_bag: DagBag, dag_id: str, task_id: str) -> BaseOperator:
	tasks = [task for task in dag_bag.get_dag(dag_id).tasks if task.task_id == task_id]
	assert len(tasks) == 1, f"Task {task_id=} not found!"
	return tasks[0]


def test_import_errors(dag_bag):
	assert not dag_bag.import_errors, f"DAGs contain import errors!"


def test_dags_contain_tasks(dag_bag):
	for dag_name, dag in dag_bag.dags.items():
		assert dag.tasks, f"DAG {dag_name=} does not have tasks!"


def test_context_manager_dag_contains_tasks(dag_bag):
	actual_task_ids = [task.task_id for task in dag_bag.get_dag("context_manager_dag").tasks]
	expected_task_ids = ["bash_task"]

	assert actual_task_ids == expected_task_ids


def test_context_manager_dag(caplog, dag_bag):
	dag = dag_bag.get_dag("context_manager_dag")
	dag.test()
	assert "echo 42" in caplog.text


def test_context_manager_dag_bash_task(dag_bag):
	dag = dag_bag.get_dag("context_manager_dag")
	dagrun = dag.create_dagrun(
        state=DagRunState.RUNNING,
		execution_date=datetime.now(timezone.utc),
        run_type=DagRunType.MANUAL,
    )
	ti = dagrun.get_task_instance(task_id="bash_task")
	ti.task = dag.get_task(task_id="bash_task")
	ti.run(ignore_ti_state=True)

	assert ti.state == TaskInstanceState.SUCCESS
