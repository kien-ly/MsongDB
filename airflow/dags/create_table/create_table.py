CREATE_TABLE = """
CREATE schema if not exists airflow;

CREATE TABLE IF NOT EXISTS airflow.dim_artist (
	artist_id VARCHAR(20) NOT null,
	artist_name VARCHAR(200),
	location_id VARCHAR(100) NOT null
);

CREATE TABLE IF NOT EXISTS airflow.dim_album (
	release VARCHAR(200),
	album_id VARCHAR(100) NOT null
);

CREATE TABLE IF NOT EXISTS airflow.dim_artist_term(
	artist_id VARCHAR(20),
	term_id VARCHAR(100) NOT null
);

CREATE TABLE IF NOT EXISTS airflow.dim_term(
	terms VARCHAR(200),
	term_id VARCHAR(100) NOT null
);

CREATE TABLE IF NOT EXISTS airflow.dim_city (
	artist_location VARCHAR(200),
	location_id VARCHAR(100) NOT null
);


CREATE TABLE IF NOT EXISTS airflow.fact_song (
	song_id VARCHAR(100) NOT null,
	artist_id VARCHAR(20) NOT null,
	title VARCHAR(400),
	duration FLOAT,
	loudness FLOAT,
	song_hotttnesss FLOAT,
	year INT8,
	start_of_fade_out FLOAT,
	end_of_fade_in FLOAT,
	album_id VARCHAR(100) NOT null,
	artist_familiarity float,
	artist_hotttnesss float,
	artist_longitude float,
	artist_latitude float
);
"""
