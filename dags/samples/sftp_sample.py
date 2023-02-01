import pendulum
import airflow
from airflow import DAG
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.sftp.operators.sftp import SFTPOperator


DAG_NAME = 'sftp_sample'

# DAG default configurations
default_args = {
    "owner" : "airflow",
    "start_date" : airflow.utils.dates.days_ago(1),
    "depends_on_post" : False,
    "email_on_failure" : False,
    "email_on_retry" : False,
}

# Timezone Setting
KST = pendulum.timezone("Asia/Seoul")

# SFTP Setting
CONN_ID = 'sftp_default'
LOCAL_PATH = '/home/bomi/airflow_tutorial/files/sftp_test.txt'
REMOTE_PATH = 'test.txt'

with DAG(
    dag_id=DAG_NAME,
    schedule_interval=None,
    default_args=default_args,
    catchup=False
) as dag:
    wait_for_input_file = SFTPSensor(
        task_id = "check-for-file",
        sftp_conn_id=CONN_ID,
        path=REMOTE_PATH,
        poke_interval=10
    )

    download_file=SFTPOperator(
        task_id="get-file",
        ssh_conn_id = CONN_ID,
        remote_filepath = REMOTE_PATH,
        local_filepath = LOCAL_PATH,
        operation="get",
        create_intermediate_dirs=True
    )

    wait_for_input_file >> download_file