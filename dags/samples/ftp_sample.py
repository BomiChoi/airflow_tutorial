import pendulum
import airflow
from airflow import DAG
from airflow.providers.ftp.sensors.ftp import FTPSensor
from airflow.providers.ftp.operators.ftp import FTPFileTransmitOperator


DAG_NAME = 'ftp_sample'

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

# FTP Setting
CONN_ID = 'ftp_default'
LOCAL_PATH = '/home/bomi/airflow_tutorial/files/ftp_test.txt'
REMOTE_PATH = 'test.txt'


with DAG(
    dag_id=DAG_NAME,
    schedule_interval=None,
    default_args=default_args,
    catchup=False
) as dag:
    wait_for_input_file = FTPSensor(
        task_id = "check-for-file",
        ftp_conn_id=CONN_ID,
        path=REMOTE_PATH,
        poke_interval=10
    )

    download_file=FTPFileTransmitOperator(
        task_id="get-file",
        remote_filepath = REMOTE_PATH,
        local_filepath = LOCAL_PATH,
        operation="get",
        create_intermediate_dirs=True
    )

    wait_for_input_file >> download_file

