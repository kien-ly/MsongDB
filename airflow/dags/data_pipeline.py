from airflow import DAG
from datetime import datetime
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor
from airflow.models import Variable
from create_table.create_table import CREATE_TABLE
from groups.group_upload_s3 import upload_s3_tasks
from groups.group_check_code import check_code_s3_tasks

BUCKET_NAME = '1msongdata'
CODE_ETL_PATH_S3 = 'etl_code/elt_pyspark.py'
CODE_S3_REDSHIFT_PATH_S3 = 'etl_code/upload_to_redshift.py'

REDSHIFT_CLUSTER = Variable.get("REDSHIFT_CLUSTER")
CONN_ID = Variable.get("CONN_ID")
REDSHIFT_CONN_ID = Variable.get("REDSHIFT_CONN_ID")
DB_LOGIN = Variable.get("LOGIN")
DB_PASS = Variable.get("PASSWORD")
DB_NAME = Variable.get("DB_NAME")

glue_crawler_config = {
        "Name": 'quangndd2-redshift-milsong-airflow',
        "Role": 'arn:aws:iam::666243375423:role/DataCamp_GlueService_Role',
        "DatabaseName": 'quangndd2-milsong-redshift',
        "Targets": {"JdbcTargets": [{"ConnectionName" : "quangndd-connector-redshift", "Path": "dev/airflow1/%"}]},
    }


with DAG(
    dag_id='data_pipeline',
    schedule='@once',
    start_date=datetime(2022, 3, 1),
    catchup=False
) as dag:

    # Upload the file
    upload = upload_s3_tasks()

    create_table_redshift_data = RedshiftDataOperator(
        task_id="create_table_redshift_data",
        cluster_identifier=REDSHIFT_CLUSTER,
        aws_conn_id=CONN_ID,
        database=DB_NAME,
        db_user=DB_LOGIN,
        sql = CREATE_TABLE,
        poll_interval=10,
        await_result=True
    )
    check_code = check_code_s3_tasks()

    crawl_redshift = GlueCrawlerOperator(
        task_id="crawl_redshift",
        config=glue_crawler_config
    )

    glue_job_etl = GlueJobOperator(
        task_id = 'glue_job_etl',
        job_name='quangndd2_etl_airflow',
        script_location=f"s3://{BUCKET_NAME}/{CODE_ETL_PATH_S3}",
        s3_bucket=BUCKET_NAME,
        iam_role_name='DataCamp_GlueService_Role',
        create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 10, "WorkerType": "G.1X"},
    )

    glue_job_s3_redshift = GlueJobOperator(
        task_id = 'glue_job_s3_redshift',
        job_name='quangndd2_s3_redshift_airflow',
        script_location=f"s3://{BUCKET_NAME}/{CODE_S3_REDSHIFT_PATH_S3}",
        s3_bucket=BUCKET_NAME,
        iam_role_name='DataCamp_GlueService_Role',

        create_job_kwargs={"GlueVersion": "4.0", "NumberOfWorkers": 10, "WorkerType": "G.1X",
                            "DefaultArguments": {'--job-bookmark-option': 'job-bookmark-enable',
                            '--TempDir': 's3://1msongdata/temp_stage'}},
    )

    [upload, create_table_redshift_data] >> check_code >> [crawl_redshift, glue_job_etl] >> glue_job_s3_redshift