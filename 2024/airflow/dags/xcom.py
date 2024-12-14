from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(dag_id="xcom_dag_jinja"):
  task1 = BashOperator(task_id="task1", bash_command="echo 42", do_xcom_push=True)
  task2 = BashOperator(task_id="task2", bash_command="echo 'echoed {{ ti.xcom_pull(task_ids='task1') }}'")

  task1 >> task2


with DAG(dag_id="xcom_dag_python"):
  task1 = PythonOperator(
    task_id="task1",
    python_callable=lambda *_: "path"
  )
  task2 = PythonOperator(
    task_id="task2",
    python_callable=lambda **context: print(context["ti"].xcom_pull(task_ids='task1')),
    provide_context=True
  )

  task1 >> task2
