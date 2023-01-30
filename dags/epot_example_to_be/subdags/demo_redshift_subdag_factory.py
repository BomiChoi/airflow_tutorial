from airflow.models import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

# Airflow connections
AWS_CONN = 'aws_conn'
REDSHIFT_CONN = 'redshift_conn'

# Airflow UI variables
DATALAKE_BUCKET = Variable.get('DATALAKE_BUCKET')
DWH_DB = Variable.get('DWH_DB')
DWH_SCHEMA = Variable.get('DWH_SCHEMA')

table_list = []

# Helper Function: Returns DAG that load S3 data to Redshift
def load_redshift_table_subdag(parent_dag_name, child_dag_name, default_args):
    with DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=default_args
    ) as dag:
        for table_name in table_list:
            t1 = PostgresOperator(
                task_id=f'truncate_{table_name}_table',
                sql=f'TRUNCATE TABLE {DWH_DB}.{table_name};',
                postgres_conn_id='redshift_conn'
            )
            t2 = S3ToRedshiftOperator(
                task_id=f'load_{table_name}_to_redshift',
                redshift_conn_id=REDSHIFT_CONN,
                aws_conn_id=AWS_CONN,
                s3_bucket=DATALAKE_BUCKET,
                s3_key=f'dwh/{table_name}/',
                schema=DWH_SCHEMA,
                table=table_name,
                copy_options=['format as parquet']
            )

            t1 >> t2

    return dag