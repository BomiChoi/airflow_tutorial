from airflow.models import DAG
from airflow.models import Variable
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

# EMR Spark Step configuration
nr_dcell_traffic_ss_prb = [
    {
        'Name': 'Invoice Fact Table Processing Step 2',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '--deploy-mode', 'cluster',
                    f's3://{PROJECT_BUCKET}/scripts/spark/nr_dcell_traffic_ss_prb.jar',
                    DATALAKE_BUCKET, ODS_DB, DWH_DB, CURRENT_YEAR, CURRENT_MONTH]
        },
    }
]

exp_access_pms_nr_mcell = [
    {
        'Name': 'Invoice Fact Table Processing Step 3',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '--deploy-mode', 'cluster',
                    f's3://{PROJECT_BUCKET}/scripts/spark/exp_access_pms_nr_mcell.jar',
                    DATALAKE_BUCKET, ODS_DB, DWH_DB, CURRENT_YEAR, CURRENT_MONTH]
        },
    }
]

procedure_list = ['HETL-0008','ETL-0012']

spark_dic = {
    'HETL-0008': nr_dcell_traffic_ss_prb,
    'ETL-0012': exp_access_pms_nr_mcell
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



def create_epot_subdag_m(parent_dag_name, child_dag_name, default_args):
    with DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=default_args
    ) as dag:
        for procedure_name in procedure_list:
            # Create EMR Cluster
            t1 = EmrCreateJobFlowOperator(
                task_id=f'create_emr_cluster_{procedure_name}',
                job_flow_overrides=JOB_FLOW_OVERRIDES,
                aws_conn_id=AWS_CONN
            )

            # Add EMR Spark step
            t2 = EmrAddStepsOperator(
                task_id=f'add_spark_step_for_{procedure_name}_table',
                job_flow_id=f"{{ task_instance.xcom_pull(task_ids='create_emr_cluster_{procedure_name}', key='return_value') }}",
                aws_conn_id=AWS_CONN,
                steps={procedure_name}
            )

            # Wait step completion
            t3 = EmrStepSensor(
                task_id=f'wait_for_{procedure_name}_complete',
                job_flow_id=f"{{ task_instance.xcom_pull('create_emr_cluster_{procedure_name}', key='return_value') }}",
                step_id=f"{{ task_istance.xcom_pull(task_ids='add_spark_step_for_{procedure_name}_table', key='return_value')[0] }}",
                aws_conn_id=AWS_CONN,
            )

            # Terminate EMR cluster
            t4 = EmrTerminateJobFlowOperator(
                task_id=f'terminate_emr_{procedure_name}_cluster',
                job_flow_id=f"{{ task_instance.xcom_pull(task_ids='create_emr_cluster_{procedure_name}', key='return_value') }}",
                aws_conn_id=AWS_CONN,
            )

            t1 >> t2 >> t3 >> t4
        
    return dag