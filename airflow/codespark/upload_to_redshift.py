import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1670380406541 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://1msongdata/clean_stage/dim_artist/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1670380406541",
)

# Script generated for node Amazon S3
AmazonS3_node1670380535411 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://1msongdata/clean_stage/dim_artist_term/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1670380535411",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://1msongdata/clean_stage/fact_song/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1670380749843 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://1msongdata/clean_stage/dim_term/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1670380749843",
)

# Script generated for node Amazon S3
AmazonS3_node1670380632347 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://1msongdata/clean_stage/dim_city/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1670380632347",
)

# Script generated for node Amazon S3
AmazonS3_node1670380257037 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://1msongdata/clean_stage/dim_album/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1670380257037",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1670380407952 = ApplyMapping.apply(
    frame=AmazonS3_node1670380406541,
    mappings=[
        ("artist_id", "string", "artist_id", "string"),
        ("artist_name", "string", "artist_name", "string"),
        ("location_id", "string", "location_id", "string"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1670380407952",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1670380538093 = ApplyMapping.apply(
    frame=AmazonS3_node1670380535411,
    mappings=[
        ("artist_id", "string", "artist_id", "string"),
        ("term_id", "string", "term_id", "string"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1670380538093",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("song_id", "string", "song_id", "string"),
        ("artist_id", "string", "artist_id", "string"),
        ("title", "string", "title", "string"),
        ("duration", "double", "duration", "double"),
        ("loudness", "double", "loudness", "double"),
        ("song_hotttnesss", "double", "song_hotttnesss", "double"),
        ("year", "bigint", "year", "long"),
        ("start_of_fade_out", "double", "start_of_fade_out", "double"),
        ("end_of_fade_in", "double", "end_of_fade_in", "double"),
        ("artist_familiarity", "double", "artist_familiarity", "double"),
        ("artist_hotttnesss", "double", "artist_hotttnesss", "double"),
        ("artist_latitude", "double", "artist_latitude", "double"),
        ("artist_longitude", "double", "artist_longitude", "double"),
        ("album_id", "string", "album_id", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1670380784041 = ApplyMapping.apply(
    frame=AmazonS3_node1670380749843,
    mappings=[
        ("terms", "string", "terms", "string"),
        ("term_id", "string", "term_id", "string"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1670380784041",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1670380634781 = ApplyMapping.apply(
    frame=AmazonS3_node1670380632347,
    mappings=[
        ("artist_location", "string", "artist_location", "string"),
        ("location_id", "string", "location_id", "string"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1670380634781",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1670380259465 = ApplyMapping.apply(
    frame=AmazonS3_node1670380257037,
    mappings=[
        ("release", "string", "release", "string"),
        ("album_id", "string", "album_id", "string"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1670380259465",
)

# Script generated for node Amazon Redshift
pre_query = "drop table if exists airflow.stage_table_2f6dbabf212e49e4bc2e778e13f162cf;create table airflow.stage_table_2f6dbabf212e49e4bc2e778e13f162cf as select * from airflow.dim_artist where 1=2;"
post_query = "begin;delete from airflow.dim_artist using airflow.stage_table_2f6dbabf212e49e4bc2e778e13f162cf where airflow.stage_table_2f6dbabf212e49e4bc2e778e13f162cf.artist_id = airflow.dim_artist.artist_id; insert into airflow.dim_artist select * from airflow.stage_table_2f6dbabf212e49e4bc2e778e13f162cf; drop table airflow.stage_table_2f6dbabf212e49e4bc2e778e13f162cf; end;"
AmazonRedshift_node1670380411346 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=ChangeSchemaApplyMapping_node1670380407952,
    catalog_connection="quangndd-connector-redshift",
    connection_options={
        "database": "dev",
        "dbtable": "airflow.stage_table_2f6dbabf212e49e4bc2e778e13f162cf",
        "preactions": pre_query,
        "postactions": post_query,
    },
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="AmazonRedshift_node1670380411346",
)

# Script generated for node Amazon Redshift
pre_query = "drop table if exists airflow.stage_table_0660f7e2fc7142e582fee98bb2578e7a;create table airflow.stage_table_0660f7e2fc7142e582fee98bb2578e7a as select * from airflow.dim_artist_term where 1=2;"
post_query = "begin;delete from airflow.dim_artist_term using airflow.stage_table_0660f7e2fc7142e582fee98bb2578e7a where airflow.stage_table_0660f7e2fc7142e582fee98bb2578e7a.term_id = airflow.dim_artist_term.term_id and airflow.stage_table_0660f7e2fc7142e582fee98bb2578e7a.artist_id = airflow.dim_artist_term.artist_id; insert into airflow.dim_artist_term select * from airflow.stage_table_0660f7e2fc7142e582fee98bb2578e7a; drop table airflow.stage_table_0660f7e2fc7142e582fee98bb2578e7a; end;"
AmazonRedshift_node1670380540170 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=ChangeSchemaApplyMapping_node1670380538093,
    catalog_connection="quangndd-connector-redshift",
    connection_options={
        "database": "dev",
        "dbtable": "airflow.stage_table_0660f7e2fc7142e582fee98bb2578e7a",
        "preactions": pre_query,
        "postactions": post_query,
    },
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="AmazonRedshift_node1670380540170",
)

# Script generated for node Redshift Cluster
pre_query = "drop table if exists airflow.stage_table_1298fbd95ae04c8a811cfe9663d357aa;create table airflow.stage_table_1298fbd95ae04c8a811cfe9663d357aa as select * from airflow.fact_song where 1=2;"
post_query = "begin;delete from airflow.fact_song using airflow.stage_table_1298fbd95ae04c8a811cfe9663d357aa where airflow.stage_table_1298fbd95ae04c8a811cfe9663d357aa.song_id = airflow.fact_song.song_id; insert into airflow.fact_song select * from airflow.stage_table_1298fbd95ae04c8a811cfe9663d357aa; drop table airflow.stage_table_1298fbd95ae04c8a811cfe9663d357aa; end;"

RedshiftCluster_node3 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=ApplyMapping_node2,
    catalog_connection="quangndd-connector-redshift",
    connection_options={
        "database": "dev",
        "dbtable": "airflow.stage_table_1298fbd95ae04c8a811cfe9663d357aa",
        "preactions": pre_query,
        "postactions": post_query,
    },
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="RedshiftCluster_node3",
)

# Script generated for node Amazon Redshift
pre_query = "drop table if exists airflow.stage_table_eda2cf8f0022447ebd99934a98e09bd8;create table airflow.stage_table_eda2cf8f0022447ebd99934a98e09bd8 as select * from airflow.dim_term where 1=2;"
post_query = "begin;delete from airflow.dim_term using airflow.stage_table_eda2cf8f0022447ebd99934a98e09bd8 where airflow.stage_table_eda2cf8f0022447ebd99934a98e09bd8.term_id = airflow.dim_term.term_id; insert into airflow.dim_term select * from airflow.stage_table_eda2cf8f0022447ebd99934a98e09bd8; drop table airflow.stage_table_eda2cf8f0022447ebd99934a98e09bd8; end;"

AmazonRedshift_node1670380786553 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=ChangeSchemaApplyMapping_node1670380784041,
    catalog_connection="quangndd-connector-redshift",
    connection_options={
        "database": "dev",
        "dbtable": "airflow.stage_table_eda2cf8f0022447ebd99934a98e09bd8",
        "preactions": pre_query,
        "postactions": post_query,
    },
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="AmazonRedshift_node1670380786553",
)

# Script generated for node Amazon Redshift
pre_query = "drop table if exists airflow.stage_table_eda84c916fdd44a5829188321943eb46;create table airflow.stage_table_eda84c916fdd44a5829188321943eb46 as select * from airflow.dim_city where 1=2;"
post_query = "begin;delete from airflow.dim_city using airflow.stage_table_eda84c916fdd44a5829188321943eb46 where airflow.stage_table_eda84c916fdd44a5829188321943eb46.location_id = airflow.dim_city.location_id; insert into airflow.dim_city select * from airflow.stage_table_eda84c916fdd44a5829188321943eb46; drop table airflow.stage_table_eda84c916fdd44a5829188321943eb46; end;"

AmazonRedshift_node1670380636609 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=ChangeSchemaApplyMapping_node1670380634781,
    catalog_connection="quangndd-connector-redshift",
    connection_options={
        "database": "dev",
        "dbtable": "airflow.stage_table_eda84c916fdd44a5829188321943eb46",
        "preactions": pre_query,
        "postactions": post_query,
    },
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="AmazonRedshift_node1670380636609",
)

# Script generated for node Amazon Redshift
pre_query = "drop table if exists airflow.stage_table_936246a36efa4a5d871af14f69e877c0;create table airflow.stage_table_936246a36efa4a5d871af14f69e877c0 as select * from airflow.dim_album where 1=2;"
post_query = "begin;delete from airflow.dim_album using airflow.stage_table_936246a36efa4a5d871af14f69e877c0 where airflow.stage_table_936246a36efa4a5d871af14f69e877c0.album_id = airflow.dim_album.album_id; insert into airflow.dim_album select * from airflow.stage_table_936246a36efa4a5d871af14f69e877c0; drop table airflow.stage_table_936246a36efa4a5d871af14f69e877c0; end;"

AmazonRedshift_node1670380262811 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=ChangeSchemaApplyMapping_node1670380259465,
    catalog_connection="quangndd-connector-redshift",
    connection_options={
        "database": "dev",
        "dbtable": "airflow.stage_table_936246a36efa4a5d871af14f69e877c0",
        "preactions": pre_query,
        "postactions": post_query,
    },
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="AmazonRedshift_node1670380262811",
)

job.commit()
