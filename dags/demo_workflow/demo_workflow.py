import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.subdag import SubDagOperator
from airflow.operators.email import EmailOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from demo_workflow.subdags.demo_ods_subdag_factory import transfer_rds_to_s3_subdag, create_ods_table_subdag
from demo_workflow.subdags.demo_dwh_subdag_factory import create_dwh_dim_table_subdag, create_dwh_fact_table_subdag
from demo_workflow.subdags.demo_redshift_subdag_factory import load_redshift_table_subdag


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

DAG_NAME = "demo_workflow"

with DAG(
    dag_id=DAG_NAME,
    schedule_interval=None,
    default_args=default_args,
    catchup=False
) as dag:

    # ODS task
    glue_transfer_rds_to_s3 = SubDagOperator(
        task_id='glue_transfer_rds_to_s3',
        subdag=transfer_rds_to_s3_subdag(DAG_NAME, 'glue_transfer_rds_to_s3', default_args),
    )

    athena_create_ods_table = SubDagOperator(
        task_id='athena_create_ods_table',
        subdag=create_ods_table_subdag(DAG_NAME, 'athena_create_ods_table', default_args),
    )

    # DWH task
    athena_create_dwh_dim_table = SubDagOperator(
        task_id='athena_create_dwh_dim_table',
        subdag=create_dwh_dim_table_subdag(DAG_NAME, 'athena_create_dwh_dim_table', default_args),
        trigger_rule="all_success"
    )

    emr_create_dwh_fact_table = SubDagOperator(
        task_id='emr_create_dwh_fact_table',
        subdag=create_dwh_fact_table_subdag(DAG_NAME, 'emr_create_dwh_fact_table', default_args),
        trigger_rule="all_success"
    )

    # Redshift task
    load_redshift_table = SubDagOperator(
        task_id='load_redshift_table',
        subdag=load_redshift_table_subdag(DAG_NAME, 'load_redshift_table', default_args),
        trigger_rule="all_success"
    )

    # Email Notification task
    send_email = EmailOperator(
        task_id="send_email",
        to="demo@example.com",
        subject="MWAA job status",
        html_content="""
            <h3>Your workflow job succeeded!
        """
    )

    # Slack Notification task
    send_slack = SlackAPIPostOperator(
        task_id="send_slack",
        token="",
        username="airflow",
        text="Your workflow job succeeded!",
        channel="#airflow-exploit"
    )

    # Execution dependency
    glue_transfer_rds_to_s3 >> athena_create_ods_table >> [athena_create_dwh_dim_table, emr_create_dwh_fact_table] >> load_redshift_table >> [send_email, send_slack]