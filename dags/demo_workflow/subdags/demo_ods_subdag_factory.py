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
ODS_DROP_ARTIST_TABLE=\
"DROP TABLE IF EXISTS artist"
ODS_CREATE_ARTIST_TABLE=\
f"""
CREATE EXTERNAL TABLE `artist`(`ArtistId` int, `Name` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://{DATALAKE_BUCKET}/ods/artist/{CURRENT_YEAR}/{CURRENT_MONTH}/'
TBLPROPERTIES('classification'='parquet', 'compressionType'='snappy', 'typeOfData'='file');
"""

ODS_DROP_ALBUM_TABLE=\
"DROP TABLE IF EXISTS album"
ODS_CREATE_ALBUM_TABLE=\
f"""
CREATE EXTERNAL TABLE `album`(`AlbumId` int, `Title` string, `ArtistId` int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://{DATALAKE_BUCKET}/ods/album/{CURRENT_YEAR}/{CURRENT_MONTH}/'
TBLPROPERTIES('classification'='parquet', 'compressionType'='snappy', 'typeOfData'='file');
"""

ODS_DROP_MEDIATYPE_TABLE=\
"DROP TABLE IF EXISTS mediatype"
ODS_CREATE_MEDIATYPE_TABLE=\
f"""
CREATE EXTERNAL TABLE `mediatype`(`MediaTypeId` int, `Name` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://{DATALAKE_BUCKET}/ods/mediatype/{CURRENT_YEAR}/{CURRENT_MONTH}/'
TBLPROPERTIES('classification'='parquet', 'compressionType'='snappy', 'typeOfData'='file');
"""

ODS_DROP_GENRE_TABLE=\
"DROP TABLE IF EXISTS genre"
ODS_CREATE_GENRE_TABLE=\
f"""
CREATE EXTERNAL TABLE `genre`(`GenreId` int, `Name` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://{DATALAKE_BUCKET}/ods/genre/{CURRENT_YEAR}/{CURRENT_MONTH}/'
TBLPROPERTIES('classification'='parquet', 'compressionType'='snappy', 'typeOfData'='file');
"""

ODS_DROP_TRACK_TABLE=\
"DROP TABLE IF EXISTS track"
ODS_CREATE_TRACK_TABLE=\
f"""
CREATE EXTERNAL TABLE `track`(`TrackId` int, `Name` string, `AlbumId` int, `MediaTypeId` int, `GenreId` int,, `Composer` string, `Milliseconds` int, `Bytes` int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://{DATALAKE_BUCKET}/ods/track/{CURRENT_YEAR}/{CURRENT_MONTH}/'
TBLPROPERTIES('classification'='parquet', 'compressionType'='snappy', 'typeOfData'='file');
"""

ODS_DROP_CUSTOMER_TABLE=\
"DROP TABLE IF EXISTS customer"
ODS_CREATE_CUSTOMER_TABLE=\
f"""
CREATE EXTERNAL TABLE `customer`(`CustomerId` int, `FirstName` string, `LastName` string, `Company` string, `Address` string, `City` string, `State` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://{DATALAKE_BUCKET}/ods/customer/{CURRENT_YEAR}/{CURRENT_MONTH}/'
TBLPROPERTIES('classification'='parquet', 'compressionType'='snappy', 'typeOfData'='file');
"""

ODS_DROP_INVOICE_TABLE=\
"DROP TABLE IF EXISTS invoice"
ODS_CREATE_INVOICE_TABLE=\
f"""
CREATE EXTERNAL TABLE `invoice`(`InvoiceId` int, `CustomerId` int, `InvoiceDate` timestamp, `BillingAddress` string, `BillingCity` string, `BillingState` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://{DATALAKE_BUCKET}/ods/invoice/{CURRENT_YEAR}/{CURRENT_MONTH}/'
TBLPROPERTIES('classification'='parquet', 'compressionType'='snappy', 'typeOfData'='file');
"""

ODS_DROP_INVOICELINE_TABLE=\
"DROP TABLE IF EXISTS invoiceline"
ODS_CREATE_INVOICELINE_TABLE=\
f"""
CREATE EXTERNAL TABLE `invoiceline`(`InvoiceLineId` int, `InvoiceId` int, `TrackId` int, `UnitPrice` decimal(10, 2), `Quantity` int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://{DATALAKE_BUCKET}/ods/invoiceline/{CURRENT_YEAR}/{CURRENT_MONTH}/'
TBLPROPERTIES('classification'='parquet', 'compressionType'='snappy', 'typeOfData'='file');
"""


table_list = ['artist', 'album', 'mediatype', 'track', 'customer', 'invoice', 'invoiceline']

drop_sql_dict = {
    'artist': ODS_DROP_ARTIST_TABLE,\
    'album': ODS_DROP_ALBUM_TABLE,\
    'mediatype': ODS_DROP_MEDIATYPE_TABLE,\
    'track': ODS_DROP_TRACK_TABLE,\
    'customer': ODS_DROP_CUSTOMER_TABLE,\
    'invoice': ODS_DROP_INVOICE_TABLE,\
    'invoiceline': ODS_DROP_INVOICELINE_TABLE,\
}
create_sql_dict = {
    'artist': ODS_CREATE_ARTIST_TABLE,\
    'album': ODS_CREATE_ALBUM_TABLE,\
    'mediatype': ODS_CREATE_MEDIATYPE_TABLE,\
    'track': ODS_CREATE_TRACK_TABLE,\
    'customer': ODS_CREATE_CUSTOMER_TABLE,\
    'invoice': ODS_CREATE_INVOICE_TABLE,\
    'invoiceline': ODS_CREATE_INVOICELINE_TABLE,\
}


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
                query=create_sql_dict[table_name],
                database=ODS_DB,
                output_location=f"s3://{PROJECT_BUCKET}/athena_query_results/",
                aws_conn_id=AWS_CONN,
                workgroup="primary"
            )

            t1 >> t2

    return dag