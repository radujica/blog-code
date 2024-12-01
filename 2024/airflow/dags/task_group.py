from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

with DAG(dag_id="taskgroup_dag") as dag:
  do_this_task = BashOperator(task_id="do_this_task", bash_command="echo 'first this'")

  with TaskGroup(group_id="grouped_tasks") as task_group:
    do_this_subtask = BashOperator(task_id="do_this_subtask", bash_command="echo 'then this'")
    do_this_other_subtask = BashOperator(task_id="do_this_other_subtask", bash_command="echo 'and this'")
    do_this_subtask >> do_this_other_subtask

  do_this_task >> task_group
