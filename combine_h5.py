import h5py
# from MillionSongDataset import h5py_getter
import h5py_getter
import os
import pandas as pd
import json
# import boto3
# from boto3.session import Session
# from pyspark.sql import SparkSession
import awswrangler as wr
# s3 = boto3.client('s3')

# spark = SparkSession.builder.config("spark.hadoop.fs.s3a.access.key", "xxxxxxxxxxx").config("spark.hadoop.fs.s3a.secret.key", "xxxxxxxxxxx")\
#     .config("spark.jars", "/home/quangndd/learn_pyspark/course-master-big-data-with-pyspark-and-aws-main/hadoop-aws-3.3.4.jar")\
#     .appName('sparkdf').getOrCreate()

# path = '/home/quangndd/learn_pyspark/course-master-big-data-with-pyspark-and-aws-main/MillionSongSubset'
path = '/home/tkien/project/1msong/millionsongsubset/MillionSongSubset'
path2 = '/home/tkien/project/1msong/millionsongsubset/MillionSongSubset/A/A/A/TRAAADZ128F9348C2E.h5'
path3 = '/home/tkien/project/1msong/msd_summary_file.h5'

data_dict = {
    'analysis_sample_rate':[],
    'artist_7digitalid':[],
    'artist_familiarity':[],
    'artist_hotttnesss':[],
    'artist_id':[],
    'artist_latitude':[],
    'artist_location':[],
    'artist_longitude':[],
    'artist_mbid':[],
    'artist_mbtags':[],
    'artist_mbtags_count':[],
    'artist_name':[],
    'artist_playmeid':[],
    'artist_terms':[],
    'artist_terms_freq':[],
    'artist_terms_weight':[],
    'audio_md5':[],
    'bars_confidence':[],
    'bars_start':[],
    'beats_confidence':[],
    'beats_start':[],
    'danceability':[],
    'duration':[],
    'end_of_fade_in':[],
    'energy':[],
    'key':[],
    'key_confidence':[],
    'loudness':[],
    'mode':[],
    'mode_confidence':[],
    'release':[],
    'release_7digitalid':[],
    'sections_confidence':[],
    'sections_start':[],
    'segments_confidence':[],
    'segments_loudness_max':[],
    'segments_loudness_max_time':[],
    'segments_loudness_start':[],
    'segments_pitches':[],
    'segments_start':[],
    'segments_timbre':[],
    'similar_artists':[],
    'song_hotttnesss':[],
    'song_id':[],
    'start_of_fade_out':[],
    'tatums_confidence':[],
    'tatums_start':[],
    'tempo':[],
    'time_signature':[],
    'time_signature_confidence':[],
    'title':[],
    'track_7digitalid':[],
    'track_id':[],
    'year':[]
}

def get_data(data, path):
    with h5py.File(path, 'r') as f:
        data['analysis_sample_rate'].append(h5py_getter.get_analysis_sample_rate(f))
        data['artist_7digitalid'].append(h5py_getter.get_artist_7digitalid(f))
        data['artist_familiarity'].append(h5py_getter.get_artist_familiarity(f))
        data['artist_hotttnesss'].append(h5py_getter.get_artist_hotttnesss(f))
        data['artist_id'].append(h5py_getter.get_artist_id(f).decode("utf-8"))
        data['artist_latitude'].append(h5py_getter.get_artist_latitude(f))
        data['artist_location'].append(h5py_getter.get_artist_location(f).decode("utf-8"))
        data['artist_longitude'].append(h5py_getter.get_artist_longitude(f))
        data['artist_mbid'].append(h5py_getter.get_artist_mbid(f).decode("utf-8"))
        data['artist_mbtags'].append(h5py_getter.get_artist_mbtags(f))
        data['artist_mbtags_count'].append(h5py_getter.get_artist_mbtags_count(f))
        data['artist_name'].append(h5py_getter.get_artist_name(f).decode("utf-8"))
        data['artist_playmeid'].append(h5py_getter.get_artist_playmeid(f))
        artist_terms = [item.decode("utf-8") for item in h5py_getter.get_artist_terms(f)]
        data['artist_terms'].append(artist_terms)
        data['artist_terms_freq'].append(h5py_getter.get_artist_terms_freq(f))
        data['artist_terms_weight'].append(h5py_getter.get_artist_terms_weight(f))
        data['audio_md5'].append(h5py_getter.get_audio_md5(f).decode("utf-8"))
        data['bars_confidence'].append(h5py_getter.get_bars_confidence(f))
        data['bars_start'].append(h5py_getter.get_bars_start(f))
        data['beats_confidence'].append(h5py_getter.get_beats_confidence(f))
        data['beats_start'].append(h5py_getter.get_beats_start(f))
        data['danceability'].append(h5py_getter.get_danceability(f))
        data['duration'].append(h5py_getter.get_duration(f))
        data['end_of_fade_in'].append(h5py_getter.get_end_of_fade_in(f))
        data['energy'].append(h5py_getter.get_energy(f))
        data['key'].append(h5py_getter.get_key(f))
        data['key_confidence'].append(h5py_getter.get_key_confidence(f))
        data['loudness'].append(h5py_getter.get_loudness(f))
        data['mode'].append(h5py_getter.get_mode(f))
        data['mode_confidence'].append(h5py_getter.get_mode_confidence(f))
        data['release'].append(h5py_getter.get_release(f).decode("utf-8"))
        data['release_7digitalid'].append(h5py_getter.get_release_7digitalid(f))
        data['sections_confidence'].append(h5py_getter.get_sections_confidence(f))
        data['sections_start'].append(h5py_getter.get_sections_start(f))
        data['segments_confidence'].append(h5py_getter.get_segments_confidence(f))
        data['segments_loudness_max'].append(h5py_getter.get_segments_loudness_max(f))
        data['segments_loudness_max_time'].append(h5py_getter.get_segments_loudness_max_time(f))
        data['segments_loudness_start'].append(h5py_getter.get_segments_loudness_start(f))
        data['segments_pitches'].append(h5py_getter.get_segments_pitches(f))
        data['segments_start'].append(h5py_getter.get_segments_start(f))
        data['segments_timbre'].append(h5py_getter.get_segments_timbre(f))
        data['similar_artists'].append(h5py_getter.get_similar_artists(f))
        data['song_hotttnesss'].append(h5py_getter.get_song_hotttnesss(f))
        data['song_id'].append(h5py_getter.get_song_id(f).decode("utf-8"))
        data['start_of_fade_out'].append(h5py_getter.get_start_of_fade_out(f))
        data['tatums_confidence'].append(h5py_getter.get_tatums_confidence(f))
        data['tatums_start'].append(h5py_getter.get_tatums_start(f))
        data['tempo'].append(h5py_getter.get_tempo(f))
        data['time_signature'].append(h5py_getter.get_time_signature(f))
        data['time_signature_confidence'].append(h5py_getter.get_time_signature_confidence(f))
        data['title'].append(h5py_getter.get_title(f).decode("utf-8"))
        data['track_7digitalid'].append(h5py_getter.get_track_7digitalid(f))
        data['track_id'].append(h5py_getter.get_track_id(f).decode("utf-8"))
        data['year'].append(h5py_getter.get_year(f))
    return data

# Loop through every directory to get .h5 file
# for directories in os.listdir(path): 
#     for middle_dir in os.listdir(os.path.join(path, directories)):
#         data = data_dict
#         for sub_dir in os.listdir(os.path.join(path, directories, middle_dir)):
#             for file in os.listdir(os.path.join(path, directories, middle_dir, sub_dir)):
#                 file_path = os.path.join(path, directories, middle_dir, sub_dir, file)
#                 print(file_path)

#                 # get all data in .h5 file and add to data dictionary
#                 data = get_data(data, file_path)
#         # print(data)
#         print(os.path.join(path, directories, middle_dir))
#         Change data dict into dataframe
#         df = pd.DataFrame(data)
#         print(df.head(2))

#         # # convert df to parquet and upload to s3 bucket
#         # wr.s3.to_parquet(
#         #     df=df,
#         #     path=f"s3://1msongdata/parquet/{directories}/{middle_dir}.parquet"
#         # )

#         ddf = spark.createDataFrame(df)
#         ddf = ddf.coalesce(1)
#         parquet_path = os.path.join(path, directories, middle_dir)
#         ddf.write.option("compression","snappy").parquet(f's3a://1msongdata/parquet/{directories}/{middle_dir}.parquet')
        

#preview data in 1 .h5 file with path2
data = data_dict

# try:
#     import cPickle as pickle
# except ImportError:  # Python 3.x
#     import pickle

data = get_data(data, path2)
# with open("sample.json", "w") as outfile:
    # json.dump(data, outfile, sort_keys=True, indent=4)
with open("summary.txt", 'w') as f: 
    for key, value in data.items(): 
        f.write('%s:%s\n' % (key, value))
print(data)  