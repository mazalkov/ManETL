from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
loader_name = "csv_to_arctic"
schedule = "0 * * * *"


with DAG(
    f"pipeline_{loader_name}",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'trigger_rule': 'all_success'
    },
    description="A simple tutorial DAG",
    schedule=schedule,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["MAN"],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
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
    p_lock >> pipeline >> p_unlock

if __name__ == "__main__":
    dag.test()