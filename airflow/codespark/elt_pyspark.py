from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import *
import hashlib
spark = SparkSession.builder.appName('sparkdf').getOrCreate()

data = spark.read.parquet("s3://1msongdata/final_parquet/*/*.parquet")
# data = spark.read.parquet("/home/quangndd/learn_pyspark/course-master-big-data-with-pyspark-and-aws-main/milsong_parquet/*.parquet")
df = data.select('song_id', 'artist_id', 'release', 'title', 'duration', 'loudness', \
    'song_hotttnesss', 'year', 'start_of_fade_out', 'end_of_fade_in', 'artist_familiarity', 'artist_hotttnesss',
    'artist_location', 'artist_terms', 'artist_name', 'artist_latitude', 'artist_longitude')

df = df.filter((func.col('song_id').isNotNull()) & (func.length(func.col('artist_name')) <= 50) & (func.length(func.col('artist_location')) <= 50))

df = df.withColumn('release', func.when((func.trim(func.col('release')) == '') | (func.col('release').isNull()), 'unknown').otherwise(func.col('release')))
df = df.withColumn('artist_location', func.when((func.trim(func.col('artist_location')) == '') | (func.col('artist_location').isNull()), 'unknown').otherwise(func.col('artist_location')))
df = df.withColumn('artist_name', func.when((func.trim(func.col('artist_name')) == '') | (func.col('artist_name').isNull()),'unknown').otherwise(func.col('artist_name')))
df = df.withColumn('artist_terms', func.when(func.size(func.col('artist_terms')) == 0, func.array(func.lit('unknown'))).otherwise(func.col('artist_terms')))

# print(df.columns)
# w = Window.partitionBy(func.lit(1)).orderBy(func.lit(1))

# df_2000 = df.filter(func.col('year') <= 2000).cache()
# df_2001 = df.filter((func.col('year') > 2000) & (func.col('year') <= 2001)).cache()
# df_2002 = df.filter((func.col('year') > 2001) & (func.col('year') <= 2002)).cache()
# df_2006 = df.filter((func.col('year') > 2005) & (func.col('year') <= 2006)).cache()
# df_2007 = df.filter((func.col('year') > 2006) & (func.col('year') <= 2007)).cache()
# df_2008 = df.filter((func.col('year') > 2007) & (func.col('year') <= 2008)).cache()
df_2009 = df.filter((func.col('year') > 2008) & (func.col('year') <= 2009)).cache()


# UDF to get hash value of a string
def hash_string(s):
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

incrUDF = func.udf(lambda s: hash_string(s), StringType())

def elt_process(df, year):
    # Create dim_album
    dim_album = df.select("release")
    # dim_album = dim_album.distinct().withColumn('album_id', func.row_number().over(w))
    dim_album = dim_album.distinct().withColumn('album_id', incrUDF(dim_album.release))
    # Add dummy value
    # newRow_album = spark.createDataFrame([('unknown','0')])
    # dim_album = dim_album.union(newRow_album).sort('album_id')
    dim_album.write.parquet(f"s3://1msongdata/clean_stage/dim_album/{year}",mode="overwrite")

    # Create dim_city
    dim_city = df.select('artist_location')
    # dim_city = dim_city.filter((func.trim(dim_city.artist_location) != '') & (dim_city.artist_location.isNotNull()))
    # dim_city = dim_city.distinct().withColumn('location_id', func.row_number().over(w))
    dim_city = dim_city.withColumn('location_id', incrUDF(dim_city.artist_location)).distinct()
    # Add dummy value
    # newRow_city = spark.createDataFrame([('unknown','0')])
    # dim_city = dim_city.union(newRow_city).sort('location_id')
    dim_city.write.parquet(f"s3://1msongdata/clean_stage/dim_city/{year}",mode="overwrite")


    # Create dim_term
    dim_term = df.select('artist_terms')
    dim_term = dim_term.select(func.explode(dim_term.artist_terms).alias('terms')).distinct()
    # dim_term = dim_term.distinct().withColumn('term_id', func.row_number().over(w))
    dim_term = dim_term.withColumn('term_id', incrUDF(dim_term.terms))
    dim_term.filter(dim_term.terms =='unknown')
    # newRow_term = spark.createDataFrame([('unknown','0')])
    # dim_term = dim_term.union(newRow_term).sort('term_id')
    dim_term.write.parquet(f"s3://1msongdata/clean_stage/dim_term/{year}",mode="overwrite")


    # Create dim_artist_term
    dim_artist_term = df.select('artist_id', 'artist_terms')
    # dim_artist_term = dim_artist_term.withColumn('artist_terms', \
    #     func.when(func.size(func.col('artist_terms')) == 0, func.array(func.lit('unknown')))\
    #     .otherwise(func.col('artist_terms')))

    dim_artist_term = dim_artist_term.select('artist_id', func.explode(dim_artist_term.artist_terms).alias('terms')).distinct()
    dim_artist_term = dim_artist_term.withColumn('term_id', incrUDF(dim_artist_term.terms)).select('artist_id', 'term_id')
    # dim_artist_term = dim_artist_term.withColumn('term_id', func.when((func.col('term_id') == 'unknown'), '0').otherwise(func.col('term_id')))\
    #     .select('artist_id', 'term_id')
    # dim_artist_term = dim_artist_term.join(dim_term, 'terms').select('artist_id', 'term_id')

    dim_artist_term.write.parquet(f"s3://1msongdata/clean_stage/dim_artist_term/{year}",mode="overwrite")


    # Create dim_artist_term
    dim_artist = df.select('artist_id', 'artist_name', 'artist_location').dropDuplicates(subset=['artist_id'])
    # dim_artist = dim_artist.withColumn('artist_location', func.when((func.trim(func.col('artist_location')) == '') | (func.col('artist_location').isNull()),\
    #     'unknown').otherwise(func.col('artist_location')))

    # dim_artist = dim_artist.withColumn('artist_name', func.when((func.trim(func.col('artist_name')) == '') | (func.col('artist_name').isNull()),\
    #     'unknown').otherwise(func.col('artist_name')))
    dim_artist = dim_artist.withColumn('location_id', incrUDF(dim_artist.artist_location)).drop('artist_location')

    # dim_artist = dim_artist.withColumn('location_id', func.when((func.col('location_id') == 'unknown'), '0').otherwise(func.col('location_id')))\
    #     .select('artist_id', 'artist_name', 'location_id').sort('artist_location')
    # dim_artist = dim_artist.join(dim_city, 'artist_location').drop('artist_location')
    dim_artist.write.parquet(f"s3://1msongdata/clean_stage/dim_artist/{year}",mode="overwrite")


    # Create fact_song
    fact_song = df.select('song_id', 'artist_id', 'release', 'title', 'duration', 'loudness', \
        'song_hotttnesss', 'year', 'start_of_fade_out', 'end_of_fade_in', 'artist_familiarity', 'artist_hotttnesss', 
        'artist_latitude', 'artist_longitude').dropDuplicates(subset=['song_id'])

    # fact_song = fact_song.withColumn('release', func.when((func.trim(func.col('release')) == '') | (func.col('release').isNull()),\
    #     'unknown').otherwise(func.col('release')))
    fact_song = fact_song.withColumn('album_id', incrUDF(fact_song.release)).drop('release')
    # fact_song = fact_song.withColumn('album_id', func.when((func.col('album_id') == 'unknown'), '0').otherwise(func.col('album_id')))\
    #     .drop('release')
    # fact_song = fact_song.join(dim_album, 'release').drop('release')
    fact_song.write.parquet(f"s3://1msongdata/clean_stage/fact_song/{year}",mode="overwrite")

    # fact_song.write.parquet(f"./fact_song",mode="overwrite")
#     return fact_song, dim_artist, dim_album, dim_artist_term, dim_city, dim_term

if __name__ == "__main__":
    elt_process(df_2009, 2009)
    print("-----------------------------DONE-----------------------------")