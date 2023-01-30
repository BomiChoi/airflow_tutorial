from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain

KST = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="epot_example_as_is", 
    schedule_interval=None, 
    start_date=datetime(2023, 1, 19, 00, tzinfo=KST)
) as dag:

    # 1. 외부 데이터 수집
    # 삼성
    with TaskGroup(group_id='extract_ss') as ex_ss:
        file_sensor_ss = FileSensor(
            task_id='check_file_ss',
            filepath=''
        )
        get_ss = BashOperator(
            task_id='cmd_ACS_PMS_NR_SS_01.sh',
            bash_command='cmd_ACS_PMS_NR_SS_01.sh'
        )
        file_sensor_ss >> get_ss

    # LG
    with TaskGroup(group_id='extract_lg') as ex_lg:
        file_sensor_lg = FileSensor(
            task_id='check_file_lg',
            filepath=''
        )
        get_lg = BashOperator(
            task_id='cmd_ACS_PMS_NR_LG_01.sh',
            bash_command='cmd_ACS_PMS_NR_LG_01.sh'
        )
        file_sensor_lg >> get_lg

    # Nokia
    with TaskGroup(group_id='extract_ns') as ex_ns:
        file_sensor_ns = FileSensor(
            task_id='check_file_ns',
            filepath=''
        )
        get_ns = BashOperator(
            task_id='cmd_ACS_PMS_NR_NS_01.sh',
            bash_command='cmd_ACS_PMS_NR_NS_01.sh'
        )
        file_sensor_ns >> get_ns


    hadoop = EmptyOperator(
        task_id="hadoop"
    )


    # 2. 내부 데이터 변환
    # 시간별
    with TaskGroup(group_id='transform_hour') as tr_h:
        transform_h1 = HiveOperator(
            task_id='nr_ss_cell_info.hpl',
            hql='nr_ss_cell_info.hpl'
        )
        transform_h2 = HiveOperator(
            task_id='NR_HCELL_TRAFFIC.hpl',
            hql='NR_HCELL_TRAFFIC.hpl'
        )
        transform_h3 = HiveOperator(
            task_id='exp_access_pms_nr_hcell.hql',
            hql='exp_access_pms_nr_hcell.hql'
        )
        transform_h1 >> transform_h2 >> transform_h3

    # 일별
    with TaskGroup(group_id='transform_day') as tr_d:
        transform_d1 = HiveOperator(
            task_id='NR_DCELL_TRAFFIC_SS_PRB.hpl',
            hql='NR_DCELL_TRAFFIC_SS_PRB.hpl'
        )
        transform_d2 = HiveOperator(
            task_id='exp_access_pms_nr_dcell.hql',
            hql='exp_access_pms_nr_dcell.hql'
        )
        transform_d1 >> transform_d2

    # 월별
    with TaskGroup(group_id='transform_month') as tr_m:
        transform_m1 = HiveOperator(
            task_id='NR_DCELL_TRAFFIC_SS_PRB.hpl',
            hql='NR_DCELL_TRAFFIC_SS_PRB.hpl'
        )
        transform_m2 = HiveOperator(
            task_id='exp_access_pms_nr_mcell.hql',
            hql='exp_access_pms_nr_mcell.hql'
        )
        transform_m1 >> transform_m2

    # 주별
    with TaskGroup(group_id='transform_week') as tr_w:
        transform_w1 = HiveOperator(
            task_id='NR_DCELL_TRAFFIC_SS_PRB.hpl',
            hql='NR_DCELL_TRAFFIC_SS_PRB.hpl'
        )
        transform_w2 = HiveOperator(
            task_id='exp_access_pms_nr_wcell.hql',
            hql='exp_access_pms_nr_wcell.hql'
        )
        transform_w1 >> transform_w2


    # 3. 내부 데이터 적재
    load_h = BashOperator(
        task_id='load_hour',
        bash_command='cmd_ODSUSER_ACS_NR_HCELL_01.sh'
    )
    load_d = BashOperator(
        task_id='load_day',
        bash_command='cmd_ODSUSER_ACS_NR_DCELL_01.sh'
    )
    load_m = BashOperator(
        task_id='load_month',
        bash_command='cmd_ODSUSER_ACS_NR_MCELL_01.sh'
    )
    load_w = BashOperator(
        task_id='load_week',
        bash_command='cmd_ODSUSER_ACS_NR_WCELL_01.sh'
    )


    mart = EmptyOperator(
        task_id="mart"
    )


    # Dependency 설정
    chain(
        [ex_ss, ex_lg, ex_ns], 
        hadoop,
        [tr_h, tr_d, tr_m, tr_w], 
        [load_h, load_d, load_m, load_w], 
        mart
    )
