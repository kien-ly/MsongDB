
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models import Variable


CONN_ID = Variable.get("CONN_ID")
BUCKET_NAME = '1msongdata'
CODE_ETL_PATH_S3 = 'etl_code/elt_pyspark.py'
CODE_S3_REDSHIFT_PATH_S3 = 'etl_code/upload_to_redshift.py'

def check_code_s3_tasks():

    with TaskGroup("check_code", tooltip="Check code existed on S3") as group:
        check_code_etl_exist = S3KeySensor(
            task_id="check_code_etl_exist",
            bucket_name=BUCKET_NAME,
            bucket_key=CODE_ETL_PATH_S3,
        )

        check_code_s3_redshift_exist = S3KeySensor(
            task_id="check_code_s3_redshift_exist",
            bucket_name=BUCKET_NAME,
            bucket_key=CODE_S3_REDSHIFT_PATH_S3,
        )

        return group