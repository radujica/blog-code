from airflow import DAG
from airflow.decorators import task, dag
from airflow.operators.bash import BashOperator

# context manager
with DAG(dag_id="context_manager_dag"):
    BashOperator(task_id="bash_task", bash_command="echo 42")

# object
dag_object = DAG(dag_id="object_dag")
BashOperator(task_id="bash_task", bash_command="echo 42", dag=dag_object)

# decorators
@task.bash
def bash_task():
    ...

@dag()
def decorators_dag():
    bash_task()

decorators_dag()
