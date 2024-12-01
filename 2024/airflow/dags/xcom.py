from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id="xcom_dag"):
  task1 = BashOperator(task_id="task1", bash_command="echo 42", do_xcom_push=True)
  task2 = BashOperator(task_id="task2", bash_command="echo 'echoed {{ ti.xcom_pull(task_ids='task1') }}'")

  task1 >> task2
