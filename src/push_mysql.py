import os

# spark df to mysql aws rds

from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
from pyspark.sql.window import Window
import time
start = time.time()
# .config("spark.jars", "/home/tkien/Lib/mysql/redshift-jdbc42-2.1.0.10.jar")\
# .config("spark.jars", "/home/tkien/Lib/mysql/mysql-connector-java-8.0.25.jar")\
spark = SparkSession.builder \
        .config("spark.jars", "/home/tkien/project/1msong/connect/redshift-jdbc42-2.1.0.10.jar") \
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

df = spark.read.csv('/home/tkien/project/1msong/data/song1.csv',schema = schema_fact_song , sep=',', dateFormat='MM/dd/yyyy',timestampFormat='yyyy-MM-dd HH:mm:ss.SSSSSS')
# df.printSchema()
# df.show()

# f = open('/home/tkien/project/1msong/data/rds.txt','r')
f = open('/home/tkien/project/1msong/emr/redshift.txt','r')
option_jdbc = [line[:-1] for line in f]
# https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
df.write.format('jdbc').options(url=option_jdbc[1],driver=option_jdbc[2], dbtable=option_jdbc[3],user=option_jdbc[4],password=option_jdbc[5]).mode('overwrite').save()
print('done')


