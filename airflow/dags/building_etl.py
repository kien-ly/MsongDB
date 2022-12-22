from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.models import Variable
from datetime import datetime

BUCKET_NAME = 'db01tung'
CODE_S3_TO_S3 = '1milionsong/code/etl_s3_to_s3.py'
FACT_S3_TO_S3 = '1milionsong/code/fact_etl_s3_to_s3.py'
CODE_S3_TO_REDSHIFT = '1milionsong/code/etl_s3_to_redshift.py'
Conn_id = Variable.get("CONN_ID")

glue_crawler_config = {
    "Name": "crawlerRedshitT",
    "Role":"arn:aws:iam::666243375423:role/DataCamp_GlueService_Role",
    "DatabaseName":"dbtung",
    "Targets": {"JdbcTargets": [{"ConnectionName" : "connection_redshift_tung", "Path": "dev/millionincremental/"}]}
}

with DAG('pipeline3', schedule='@daily', start_date= datetime(2022,1,1), catchup=False) as dag:
    glueJob_s3_to_s3 =  GlueJobOperator(
        task_id = 's3_to_s3',
        aws_conn_id=Conn_id,
        job_name='dim_etl_everyYear',
        script_location=f"s3://{BUCKET_NAME}/{CODE_S3_TO_S3}",
        s3_bucket = BUCKET_NAME,
        iam_role_name='DataCamp_GlueService_Role',
        create_job_kwargs= {"GlueVersion": "3.0","NumberOfWorkers": 10, "WorkerType": "G.1X"}
    )

    glueJob_factS3_to_s3 = GlueJobOperator(
        task_id='fact_s3_to_s3',
        aws_conn_id=Conn_id,
        job_name='fact_etl_everyYear',
        script_location=f"s3://{BUCKET_NAME}/{FACT_S3_TO_S3}",
        s3_bucket=BUCKET_NAME,
        iam_role_name='DataCamp_GlueService_Role',
        create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 10, "WorkerType": "G.1X"}
    )

    crawl_redshift = GlueCrawlerOperator(
        task_id="crawl_redshift",
        aws_conn_id=Conn_id,
        config=glue_crawler_config
    )

    glueJob_s3_to_redshift = GlueJobOperator(
        task_id='s3_to_redshift',
        aws_conn_id=Conn_id,
        job_name='etl_s3_to_redshift_tung',
        script_location=f"s3://{BUCKET_NAME}/{CODE_S3_TO_REDSHIFT}",
        s3_bucket=BUCKET_NAME,
        iam_role_name='DataCamp_GlueService_Role',
        create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 10, "WorkerType": "G.1X",
                           "DefaultArguments": {'--job-bookmark-option': 'job-bookmark-enable'}}
    )

    glueJob_s3_to_s3 >> glueJob_factS3_to_s3 >> crawl_redshift >> glueJob_s3_to_redshift