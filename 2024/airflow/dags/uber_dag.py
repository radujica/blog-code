from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor


parent_dag = DAG(dag_id="parent_dag")
do_this_task = BashOperator(task_id="do_this_task", bash_command="echo 'first this'", dag=parent_dag)

child_dag = DAG(dag_id="child_dag")
wait_for_upstream_dag_task = ExternalTaskSensor(
    task_id="wait_task",
    external_dag_id=parent_dag.dag_id,
    external_task_id=do_this_task.task_id,
    dag=child_dag
)
then_this_task = BashOperator(task_id="then_this_task", bash_command="echo 'then this'", dag=child_dag)
wait_for_upstream_dag_task >> then_this_task
