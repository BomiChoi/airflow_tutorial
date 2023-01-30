from airflow.models import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor


# Airflow connections
AWS_CONN = 'aws_default'

# Airflow UI variables
CURRENT_YEAR = Variable.get('CURRENT_YEAR')
CURRENT_MONTH = Variable.get('CURRENT_MONTH')
DATALAKE_BUCKET = Variable.get('DATALAKE_BUCKET')
PROJECT_BUCKET = Variable.get('PROJECT_BUCKET')
ODS_DB = Variable.get('ODS_DB')
DWH_DB = Variable.get('DWH_DB')

# DW SQLs
DWH_DROP_DIM_TRACK_TABLE="DROP TABLE IF EXISTS dim_track"
DWH_CREATE_DIM_TRACK_TABLE=\
f"""
WITH (
    external_location = 's3://{DATALAKE_BUCKET}/dwh/dim_track/',
    format = 'Parquet',
    parquet_compression = 'SNAPPY'
) AS
SELECT
tr.trackid as "track_id",
tr.name, tr.composer,
tr.milliseconds,
tr.bytes, tr.unitprice,
ar.name as "artist",
al.title as "album",
ge.name as "genre",
me.name as "media_type"
FROM {ODS_DB}.artist ar
    INNER JOIN {ODS_DB}.album al ON ar.artistid = al.artistid
    INNER JOIN {ODS_DB}.track tr ON al.albumid = tr.albumid
    INNER JOIN {ODS_DB}.genre ge ON tr.genreid = ge.genreid
    INNER JOIN {ODS_DB}.mediatype me ON tr.mediatypeid = me.mediatypeid
"""

DWH_DROP_DIM_CUSTOMER_TABLE="DROP TABLE IF EXISTS dim_customer"
DWH_CREATE_DIM_CUSTOMER_TABLE=\
f"""
CREATE TABLE dim_customer
WITH (
    external_location = 's3://{DATALAKE_BUCKET}/dwh/dim_customer/',
    format = 'Parquet',
    parquet_compression = 'SNAPPY'
) AS
SELECT customerid as "customer_id", firstname as "first_name", lastname as "last_name",
        company, address, cit, state, country, postalcode as "postal_code", phone,
        fax, email, supportrepid as "support_rep_id"
FROM {ODS_DB}.customer
"""

DWH_DROP_DIM_INVOICE_TABLE="DROP TABLE IF EXISTS dim_invoice"
DWH_CREATE_DIM_INVOICE_TABLE=\
f"""
CREATE TABLE dim_invoice
WITH (
    external_location = 's3://{DATALAKE_BUCKET}/dwh/dim_invoice/',
    format = 'Parquet',
    parquet_compression = 'SNAPPY'
) AS
SELECT customerid as "customer_id", firstname as "first_name", lastname as "last_name",
        company, address, cit, state, country, postalcode as "postal_code", phone,
        fax, email, supportrepid as "support_rep_id"
FROM {ODS_DB}.customer
"""


table_list = ['dim_track', 'dim_customer', 'dim_invoice']

drop_sql_dict = {
    'dim_track': DWH_DROP_DIM_TRACK_TABLE, \
    'dim_customer': DWH_DROP_DIM_CUSTOMER_TABLE, \
    'dim_invoice': DWH_DROP_DIM_INVOICE_TABLE
}
create_sql_dict = {
    'dim_track': DWH_CREATE_DIM_TRACK_TABLE, \
    'dim_customer': DWH_CREATE_DIM_CUSTOMER_TABLE, \
    'dim_invoice': DWH_CREATE_DIM_INVOICE_TABLE
}

# EMR Cluster configuration
JOB_FLOW_OVERRIDES = {
    'Name': 'demo-spark-cluster',
    'ReleaseLabel': 'emr-6.2.0',
    'Applications': [
        {
            'Name': 'Spark'
        }
    ],
    'Configurations': [
        {
            'Classification': 'spark-hive-site',
            'Properties': {
                'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueataCatalogHiveClientFactory'
            }
        }
    ],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master nodes',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Slave nodes',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    'VisibleToAllUsers': True,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'Tags': [
        {
            'Key': 'Project',
            'Value': 'Airflow Demo'
        }
    ]
}

# EMR Spark Step configuration
EMR_STEPS = [
    {
        'Name': 'Invoice Fact Table Processing Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '--deploy-mode', 'cluster', \
                    f's3://{PROJECT_BUCKET}/scripts/spark/create_dwh_fact_invoice_table.py', \
                    DATALAKE_BUCKET, ODS_DB, DWH_DB, CURRENT_YEAR, CURRENT_MONTH]
        },
    }
]

# Helper Function: Returns DAG that create DW dimension tables
def create_dwh_dim_table_subdag(parent_dag_name, child_dag_name, default_args):
    with DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=default_args
    ) as dag:
        for table_name in table_list:
            if table_name == 'fact_invoice':
                s3_prefix = f'dwh/{table_name}/year={CURRENT_YEAR}/month={CURRENT_MONTH}/'
            else:
                s3_prefix = f'dwh/{table_name}/'

            t1 = S3DeleteObjectsOperator(
                task_id=f'clear_{table_name}_data',
                bucket=DATALAKE_BUCKET,
                prefix=s3_prefix,
                aws_conn_id=AWS_CONN
            )
            t2 = AWSAthenaOperator(
                task_id=f'drop_{table_name}_table',
                query=drop_sql_dict[table_name],
                database=DWH_DB,
                output_location=f's3://{PROJECT_BUCKET}/athena_query_results/',
                aws_conn_id=AWS_CONN,
                workgroup='primary'
            )
            t3 = AWSAthenaOperator(
                task_id=f'create_{table_name}_table',
                query=create_sql_dict[table_name],
                database=DWH_DB,
                output_location=f's3://{PROJECT_BUCKET}/athena_query_results/',
                aws_conn_id=AWS_CONN,
                workgroup='primary'
            )

            t1 >> t2 >> t3

    return dag

# Helper Function: Returns DAG that create DW fact table
def create_dwh_fact_table_subdag(parent_dag_name, child_dag_name, default_args):
    with DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=default_args
    ) as dag:
        # Create EMR Cluster
        t1 = EmrCreateJobFlowOperator(
            task_id='create_emr_cluster',
            job_flow_overrides=JOB_FLOW_OVERRIDES,
            aws_conn_id=AWS_CONN
        )

        # Add EMR Spark step
        t2 = EmrAddStepsOperator(
            task_id='add_spark_step_for_fact_table',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
            aws_conn_id=AWS_CONN,
            steps=EMR_STEPS
        )

        # Wait step completion
        t3 = EmrStepSensor(
            task_id='wait_for_step_complete',
            job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
            step_id="{{ task_istance.xcom_pull(task_ids='add_spark_step_for_fact_table', key='return_value')[0] }}",
            aws_conn_id=AWS_CONN,
        )

        # Terminate EMR cluster
        t4 = EmrTerminateJobFlowOperator(
            task_id='terminate_emr_cluster',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
            aws_conn_id=AWS_CONN,
        )

        t1 >> t2 >> t3 >> t4
    
    return dag