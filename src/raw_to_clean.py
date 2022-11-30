from pyspark.sql import SparkSession, functions as func
from pyspark.sql.window import Window
spark = SparkSession.builder.appName('sparkdf').getOrCreate()

df = spark.read.parquet("s3://1msongdata/final_parquet/*/*.parquet")
print(df.columns)
w = Window.partitionBy(func.lit(1)).orderBy(func.lit(1))

# Create dim_album
dim_album = df.select("release")
dim_album = dim_album.distinct().withColumn('album_id', func.row_number().over(w))
# Add dummy value
newRow_album = spark.createDataFrame([('unknown',-1)])
dim_album = dim_album.union(newRow_album).sort('album_id')
dim_album.write.parquet("s3://1msongdata/clean_test/dim_album",mode="overwrite")


# Create dim_city
dim_city = df.select('artist_location').filter((df.artist_location != '') | (df.artist_location.isNotNull()))
dim_city = dim_city.distinct().withColumn('location_id', func.row_number().over(w))
# Add dummy value
newRow_city = spark.createDataFrame([('unknown',-1)])
dim_city = dim_city.union(newRow_city).sort('location_id')
dim_city.write.parquet("s3://1msongdata/clean_test/dim_city",mode="overwrite")

# Create dim_term
dim_term = df.filter((func.size(df.artist_terms) != 0)).select(func.explode(df.artist_terms).alias('terms'))
dim_term = dim_term.distinct().withColumn('term_id', func.row_number().over(w))
newRow_term = spark.createDataFrame([('unknown',-1)])
dim_term = dim_term.union(newRow_term).sort('term_id')
dim_term.write.parquet("s3://1msongdata/clean_test/dim_term",mode="overwrite")


# Create dim_artist_term
dim_artist_term = df.select('artist_id', 'artist_terms')
dim_artist_term = dim_artist_term.withColumn('artist_terms', \
    func.when(func.size(func.col('artist_terms')) == 0, func.array(func.lit('unknown')))\
    .otherwise(func.col('artist_terms')))

dim_artist_term = dim_artist_term.select('artist_id', func.explode(dim_artist_term.artist_terms).alias('terms')).distinct()
dim_artist_term = dim_artist_term.join(dim_term, 'terms').select('artist_id', 'term_id')
dim_artist_term.write.parquet("s3://1msongdata/clean_test/dim_artist_term",mode="overwrite")


# Create dim_artist_term
dim_artist = df.select('artist_id', 'artist_name', 'artist_location', 'artist_familiarity', 'artist_hotttnesss', \
                        'artist_latitude', 'artist_longitude')
dim_artist = dim_artist.withColumn('artist_location', func.when((func.col('artist_location') == '') | (func.col('artist_location').isNull()),\
    'unknown').otherwise(func.col('artist_location')))
dim_artist = dim_artist.join(dim_city, 'artist_location').drop('artist_location')
dim_artist.write.parquet("s3://1msongdata/clean_test/dim_artist",mode="overwrite")


# Create fact_song
fact_song = df.select('song_id', 'artist_id', 'release', 'title', 'duration', 'loudness', \
    'song_hotttnesss', 'year', 'start_of_fade_out', 'end_of_fade_in')

fact_song = fact_song.withColumn('release', func.when((func.col('release') == '') | (func.col('release').isNull()),\
    'unknown').otherwise(func.col('release')))
fact_song = fact_song.join(dim_album, 'release').drop('release')
fact_song.write.parquet("s3://1msongdata/clean_test/fact_song",mode="overwrite")
