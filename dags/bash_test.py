from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

KST = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="bash_test", 
    schedule_interval=None, 
    start_date=datetime(2023, 1, 18, 00, tzinfo=KST),
    template_searchpath="/home/bomi/airflow_tutorial/shell" # Shell 파일 경로
) as dag:
    hello = BashOperator(
        task_id='hello',
        bash_command='hello.sh',
        cwd="/home/bomi/airflow_tutorial/shell"
    )
    params_test = BashOperator(
        task_id='params_test',
        bash_command='params_test.sh',
        params={'NOW' : datetime.now().strftime("%Y/%m/%d %H:%M:%S")}, # 현재 시각
        cwd="/home/bomi/airflow_tutorial/shell"
    )
    
    hello >> params_test