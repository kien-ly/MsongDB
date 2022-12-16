
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from hooks.s3.s3 import S3Hook
from airflow.models import Variable


CONN_ID = Variable.get("CONN_ID")
BUCKET_NAME = '1msongdata'
CODE_ETL_PATH = '/opt/airflow/codespark/elt_pyspark.py'
CODE_ETL_PATH_S3 = 'etl_code/elt_pyspark.py'
CODE_S3_REDSHIFT_PATH = '/opt/airflow/codespark/upload_to_redshift.py'
CODE_S3_REDSHIFT_PATH_S3 = 'etl_code/upload_to_redshift.py'

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook(CONN_ID)
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name, replace=True)

def upload_s3_tasks():

    with TaskGroup("uploads", tooltip="Upload code to s3 bucket") as group:
        upload_code_etl_to_s3 = PythonOperator(
            task_id='upload_code_etl_to_s3',
            python_callable=upload_to_s3,
            op_kwargs={
                'filename': CODE_ETL_PATH,
                'key': CODE_ETL_PATH_S3,
                'bucket_name': BUCKET_NAME
            }
        )

        upload_code_s3_redshift_to_s3 = PythonOperator(
            task_id='upload_code_s3_redshift_to_s3',
            python_callable=upload_to_s3,
            op_kwargs={
                'filename': CODE_S3_REDSHIFT_PATH,
                'key': CODE_S3_REDSHIFT_PATH_S3,
                'bucket_name': BUCKET_NAME
            }
        )

        return group