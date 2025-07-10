from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    "my_dag_v_1_0_0",
    start_date=datetime(2025, 7, 7),
    schedule_interval="@daily",
    catchup=False,
    tags=["example"]  
) as dag:
    task_a = BashOperator(
        task_id="task_a",
        bash_command = "echo 'task_a'"
    )

    task_b = BashOperator(
        task_id="task_b",
        email = ["gulvera2003@gmail.com"],
        email_on_retry=False,
        email_on_failure= False,
        retries = 3,
        retry_delay = timedelta(seconds=10),
        bash_command = "echo '{{ti.try_number}}' && exit 1"
    )

    task_a>> task_b

