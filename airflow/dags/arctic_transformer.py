from airflow import DAG
from airflow.operators.bash import BashOperator

IMAGE = None #TODO: implement the image

"""Example DAG demonstrating the usage of the BashOperator."""

import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
loader_name = "arctic_transformer"
schedule = "30 */2 * * *"



with DAG(
    dag_id=f"pipeline_{loader_name}",
    schedule=schedule,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["MAN"]
) as dag:
    p_unlock = BashOperator(
        task_id="p_unlock",
        bash_command="echo unlocking {{ task_instance_key_str }}"
    )

    pipeline = BashOperator(
        task_id=f"p_{loader_name}",
        bash_command='echo running_loader',
    )

    # [START howto_operator_bash]
    p_lock = BashOperator(
        task_id="p_lock",
        bash_command="echo locking {{ task_instance_key_str }}"
    )

    p_unlock >> pipeline >> p_lock



if __name__ == "__main__":
    dag.test()