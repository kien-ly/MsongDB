## Data pipeline


![](https://github.com/neikyllykien/MsongDB/blob/main/img/a2.png)
*pipeline*


## Config AWS, redshift
Vì lý do bảo mật không nên điền thông tin trực tiếp vào file python dag. Lưu vào file text theo từng dòng theo thứ tự:

```
BUCKET_NAME = config[0]
CODE_ETL_PATH_S3 = config[1]
CODE_S3_REDSHIFT_PATH_S3 = config[2]
REDSHIFT_CLUSTER = config[3]
CONN_ID = config[4]
REDSHIFT_CONN_ID = config[5]
DB_LOGIN = config[6]
DB_PASS = config[7]
DB_NAME
```

Duong dan file x.txt: /opt/airflow/dags/x.txt

Voi trong dockerfile set:
ENV AIRFLOW_HOME=/opt/airflow

Chúng ta có thể set giá trị trong UI airflow như trong hình

![](https://github.com/neikyllykien/MsongDB/blob/main/img/a-variable.png)
*airflow variable*

Thêm nữa ta phải khai báo kết nối đến aws và redshift 
![](https://github.com/neikyllykien/MsongDB/blob/main/img/a-connect.png)
*airflow connection*

## glue_crawler_config
```
glue_crawler_config = {
        "Name": 'quangndd2-redshift-milsong-airflow',
        "Role": 'arn:aws:iam::666243375423:role/DataCamp_GlueService_Role',
        "DatabaseName": 'quangndd2-milsong-redshift',
        "Targets": {"JdbcTargets": [{"ConnectionName" : "quangndd-connector-redshift", "Path": "dev/airflow/%"}]},
    }
```
Để kết nối glue job đã được cấu hình sẵn trên AWS. 


![](https://github.com/neikyllykien/MsongDB/blob/main/img/a-redshift-connect.png)
*redshift-connect*



# Cấu trúc pipeline
<p>
    <img src="https://github.com/neikyllykien/MsongDB/blob/main/img/a2.png" alt>
    <em>pipeline</em>
</p>

```
[upload, create_table_redshift_data] >> check_code >> [crawl_redshift, glue_job_etl] >> glue_job_s3_redshift
```

### upload
``` upload = upload_s3_tasks() ```

#### upload_code_etl_to_s3
```
upload_code_etl_to_s3 = PythonOperator(
            task_id='upload_code_etl_to_s3',
            python_callable=upload_to_s3,
            op_kwargs={
                'filename': CODE_ETL_PATH,
                'key': CODE_ETL_PATH_S3,            
                'bucket_name': BUCKET_NAME
            }
        )
```
#### upload_code_s3_redshift_to_s3
Giống task trên


# Pipeline khác đơn giản hơn:

``` glueJob_s3_to_s3 >> glueJob_factS3_to_s3 >> crawl_redshift >> glueJob_s3_to_redshift ```

