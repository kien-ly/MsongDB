import os
import boto3
import pandas as pd

# spark df to mysql aws rds

from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
from pyspark.sql.window import Window
# import mysql.connector
import pymysql
import time
import datetime


start = time.time()
# .config("spark.jars", "/home/tkien/Lib/mysql/redshift-jdbc42-2.1.0.10.jar")\
# .config("spark.jars", "/home/tkien/Lib/mysql/mysql-connector-java-8.0.25.jar")\
spark = SparkSession.builder \
        .config("spark.jars", "/home/tkien/Lib/mysql/mysql-connector-java-8.0.25.jar") \
        .master("local[*]")\
        .appName('sparkdf') \
        .getOrCreate()


schema_fact_song = StructType([
                    StructField('song_id', StringType(), True),
                    StructField('artist_id', StringType(), True),
                    StructField('title', StringType(), True),
                    StructField('duration', DoubleType(), True),
                    StructField('loudness', DoubleType(), True),
                    StructField('song_hotttnesss', DoubleType(), True),
                    StructField('year', IntegerType(), True),
                    StructField('start_of_fade_out', DoubleType(), True),
                    StructField('end_of_fade_in', DoubleType(), True),
                    StructField('album_id', IntegerType(), True)
])

s3 = boto3.client('s3') 
bucket = 'kienlt24'
file_csv = 'song1.csv'
obj = s3.get_object(Bucket= bucket, Key= file_csv) 
# file_csv = '/home/tkien/project/1msong/data/song1.csv'

# df = spark.read.csv(file_csv,schema = schema_fact_song , sep=',', dateFormat='MM/dd/yyyy',timestampFormat='yyyy-MM-dd HH:mm:ss.SSSSSS')
df = spark.read.csv(obj['Body'],schema = schema_fact_song , sep=',', dateFormat='MM/dd/yyyy',timestampFormat='yyyy-MM-dd HH:mm:ss.SSSSSS')
# df.printSchema()
df.show(5)



# f = open('/home/tkien/project/1msong/emr/rds.txt','r')
# # f = open('/home/tkien/project/1msong/emr/redshift.txt','r')
# option_jdbc = [line[:-1] for line in f]
# # https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html

# # df.write.format('jdbc').options(url=option_jdbc[1],driver=option_jdbc[2], dbtable=option_jdbc[3],user=option_jdbc[4],password=option_jdbc[5]).mode('overwrite').save()
# # df.write.format('jdbc').options(url=option_jdbc[1],driver=option_jdbc[2], dbtable="song",user=option_jdbc[4],password=option_jdbc[5]).mode('overwrite').save()
# # print('up to song')
# df.write.format('jdbc').options(url=option_jdbc[1],driver=option_jdbc[2], dbtable="song_test",user=option_jdbc[4],password=option_jdbc[5]).mode('overwrite').save()
# print('done up to song_test')

# # df.write.format("csv").mode('overwrite').save("/home/tkien/project/1msong/data/test.csv")
# # df.write.parquet("/home/tkien/project/1msong/data/test", mode='append' )


# ## Run query INSERT INTO  
# #establishing the connection
# conn=pymysql.connect(host=option_jdbc[0],user=option_jdbc[4],password=option_jdbc[5],database='msongdb', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
# #Creating a cursor object using the cursor() method
# cursor = conn.cursor()
# cursor.connection.autocommit(True)
# #Executing an MYSQL function using the execute() method
# cursor.execute("INSERT INTO song \
# SELECT * FROM song_test a \
# WHERE NOT EXISTS (SELECT 1 FROM song X WHERE a.song_id  = X.song_id)")
# print('done query')
# #Closing the connection
# conn.close()



end = time.time()
print(end - start)