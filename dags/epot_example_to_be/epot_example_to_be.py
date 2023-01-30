import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.subdag import SubDagOperator
from epot_example_to_be.subdags.demo_ods_subdag_factory import transfer_rds_to_s3_subdag
from epot_example_to_be.subdags.epot_pr_0014_subdags_h import create_epot_subdag_h
from epot_example_to_be.subdags.epot_pr_0014_subdags_d import create_epot_subdag_d
from epot_example_to_be.subdags.epot_pr_0014_subdags_m import create_epot_subdag_m
from epot_example_to_be.subdags.epot_pr_0014_subdags_w import create_epot_subdag_w
from epot_example_to_be.subdags.demo_redshift_subdag_factory import load_redshift_table_subdag


# UI variables
START_DATE = Variable.get('START_DATE')
END_DATE = Variable.get('END_DATE')
DWH_DB = Variable.get('DWH_DB')
DATALAKE_BUCKET = Variable.get('DATALAKE_BUCKET')
AWS_REGION = Variable.get('AWS_REGION')

# DAG default configurations
default_args = {
    "owner" : "airflow",
    "start_date" : airflow.utils.dates.days_ago(1),
    "depends_on_post" : False,
    "email_on_failure" : False,
    "email_on_retry" : False,
}

DAG_NAME = "epot_example_to_be"

with DAG(
    dag_id=DAG_NAME,
    schedule_interval=None,
    default_args=default_args,
    catchup=False
) as dag:

    # ODS task
    glue_transfer_rds_to_s3 = SubDagOperator(
        task_id='epot_0014_glue_transfer_rds_to_s3',
        subdag=transfer_rds_to_s3_subdag(DAG_NAME, 'epot_0014_glue_transfer_rds_to_s3', default_args),
    )

    # EMR task
    emr_create_ss_celltbl_h = SubDagOperator(
        task_id='epot_0014_emr_create_ss_celltbl_h',
        subdag=create_epot_subdag_h(DAG_NAME, 'epot_0014_emr_create_ss_celltbl_h', default_args),
        trigger_rule="all_success"
    )
    
    emr_create_ss_celltbl_d = SubDagOperator(
        task_id='epot_0014_emr_create_ss_celltbl_d',
        subdag=create_epot_subdag_d(DAG_NAME, 'epot_0014_emr_create_ss_celltbl_d', default_args),
        trigger_rule="all_success"
    )
    
    emr_create_ss_celltbl_m = SubDagOperator(
        task_id='epot_0014_emr_create_ss_celltbl_m',
        subdag=create_epot_subdag_m(DAG_NAME, 'epot_0014_emr_create_ss_celltbl_m', default_args),
        trigger_rule="all_success"
    )
    
    emr_create_ss_celltbl_w = SubDagOperator(
        task_id='epot_0014_emr_create_ss_celltbl_w',
        subdag=create_epot_subdag_w(DAG_NAME, 'epot_0014_emr_create_ss_celltbl_w', default_args),
        trigger_rule="all_success"
    )
    
    # Redshift task
    load_redshift_table = SubDagOperator(
        task_id='epot_0014_load_redshift_table',
        subdag=load_redshift_table_subdag(DAG_NAME, 'epot_0014_load_redshift_table', default_args),
        trigger_rule="all_success"
    )

    # Execution dependency
    glue_transfer_rds_to_s3 >> [emr_create_ss_celltbl_h, emr_create_ss_celltbl_d, emr_create_ss_celltbl_m, emr_create_ss_celltbl_w ] >> load_redshift_table