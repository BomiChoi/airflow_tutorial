from airflow.models import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator


# Airflow connections
AWS_CONN = 'aws_default'

# Airflow UI variables
ODS_DB = Variable.get('ODS_DB')
AWS_REGION = Variable.get('AWS_REGION')
CURRENT_YEAR = Variable.get('CURRENT_YEAR')
CURRENT_MONTH = Variable.get('CURRENT_MONTH')
DATALAKE_BUCKET = Variable.get('DATALAKE_BUCKET')
PROJECT_BUCKET = Variable.get('PROJECT_BUCKET')

# ODS SQLs 


table_list = []

drop_sql_dict = {}
create_sql_dict = {}


# Helper Function : Returns DAG that ingest data from RDS to S3 ODS tables
def transfer_rds_to_s3_subdag(parent_dag_name, child_dag_name, default_args):
    with DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=default_args
    ) as dag:

        for table_name in table_list:
            t1 = S3DeleteObjectsOperator(
                    task_id=f'clear_{table_name}_data_in_s3',
                    bucket=DATALAKE_BUCKET,
                    prefix=f'ods/{table_name}/{CURRENT_YEAR}/{CURRENT_MONTH}',
                    aws_conn_id=AWS_CONN
                )
            t2 = AwsGlueJobOperator(
                task_id=f'transfer_{table_name}_data_to_s3',
                job_name=f'chinhook-{table_name}-table-to-s3',
                script_args={"--datalake_bucket": DATALAKE_BUCKET, \
                            "--year_partition_key": CURRENT_YEAR, \
                            "--month_partition_key": CURRENT_MONTH},
                num_of_dpus=5,
                region_name=AWS_REGION
            )

            t1 >> t2

    return dag

# Helper Function: Returns DAG that create ODS tables in Glue Catalog
def create_ods_table_subdag(parent_dag_name, child_dag_name, default_args):
    with DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=default_args
    ) as dag:
        for table_name in table_list:
            t1 = AWSAthenaOperator(
                task_id=f'drop_{table_name}_table',
                query=drop_sql_dict[table_name],
                database=ODS_DB,
                output_location=f"s3://{PROJECT_BUCKET}/athena_query_results/",
                aws_conn_id=AWS_CONN,
                workgroup="primary"
            )
            t2 = AWSAthenaOperator(
                task_id=f'create_{table_name}_table',
                query=drop_sql_dict[table_name],
                database=ODS_DB,
                output_location=f"s3://{PROJECT_BUCKET}/athena_query_results/",
                aws_conn_id=AWS_CONN,
                workgroup="primary"
            )

            t1 >> t2

    return dag