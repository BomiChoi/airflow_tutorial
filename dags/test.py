from datetime import datetime
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

KST = pendulum.timezone("Asia/Seoul")

with DAG(dag_id="test", start_date=datetime(2023, 1, 15, 14, tzinfo=KST), schedule="* * * * *") as dag:
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    @task()
    def airflow():
        print("airflow")

    hello >> airflow()