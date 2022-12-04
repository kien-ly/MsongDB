from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
from pyspark.sql.window import Window
import time
start = time.time()
#do something

spark = SparkSession.builder \
        .config("spark.jars", "/home/tkien/Lib/mysql/mysql-connector-java-8.0.25.jar")\
        .master("local[*]")\
        .appName('sparkdf') \
        .getOrCreate()

df = spark.read.parquet('/home/tkien/project/1msong/data/raw.parquet')
# print(df.columns)
# save_cols = '/home/tkien/project/1msong/schema/cols.txt'
# with open(save_cols, 'w') as f:    
#     # f.write("%s\n" % df.columns)
#     for col in df.columns:
#         f.write("%s\n" % col)

w = Window.partitionBy(func.lit(1)).orderBy(func.lit(1))

# Create dim_album
dim_album = df.select("release")
dim_album = dim_album.distinct().withColumn('album_id', func.row_number().over(w))
# Add dummy value
newRow_album = spark.createDataFrame([('unknown',-1)])
dim_album = dim_album.union(newRow_album).sort('album_id')
# dim_album.write.parquet("s3://1msongdata/clean_test/dim_album",mode="overwrite")

# Create fact_song
schema_fact_song = StructType([
                    StructField('song_id', StringType(), True),
                    StructField('artist_id', StringType(), True),
                    StructField('release', StringType(), True),
                    StructField('title', StringType(), True),
                    StructField('duration', DoubleType(), True),
                    StructField('loudness', DoubleType(), True),
                    StructField('artist_hotttnesss', DoubleType(), True),
                    StructField('year', IntegerType(), True),
                    StructField('start_of_fade_out', DoubleType(), True),
                    StructField('end_of_fade_in', DoubleType(), True)
])

fact_song = df.select('song_id', 'artist_id', 'release', 'title', 'duration', 'loudness', \
    'song_hotttnesss', 'year', 'start_of_fade_out', 'end_of_fade_in')


fact_song = spark.createDataFrame(fact_song.collect(), schema_fact_song)

fact_song = fact_song.withColumn('release', func.when((func.col('release') == '') | (func.col('release').isNull()),\
    'unknown').otherwise(func.col('release')))
fact_song = fact_song.join(dim_album, 'release').drop('release')
fact_song.na.fill(value=0)
# fact_song.write.parquet("s3://1msongdata/clean_test/fact_song",mode="overwrite")
# fact_song.show(10)
# fact_song.write.csv('/home/tkien/project/1msong/data/fact_song.csv', header=True, mode='overwrite')
fact_song.write.csv('/home/tkien/project/1msong/data/fact_song.csv', header=False, mode='overwrite')
# fact_song1 =fact_song.select(1)
fact_song.show(5)
# fact_song1.show()
# fact_song.write.format('jdbc').options(
#     url='jdbc:mysql://localhost/database_name',
#     driver='com.mysql.jdbc.Driver',
#     dbtable='DestinationTableName',
#     user='your_user_name',
#     password='your_password').mode('append').save()

end = time.time()
temp = end-start
print(temp)