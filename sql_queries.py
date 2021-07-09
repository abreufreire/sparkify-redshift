#!/usr/bin/env python
# -*- coding: utf-8 -*-

import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


'''
redshift tables notes:
DISTKEY: should be column that appears in most/bigger JOINs;
         the value in DISTKEY column is hashed & values are distributed based on the hash
         (this value is used to distribute the data over any available slices).
SORTKEY: should be column more useful to sort rows in each slice;
         primary key of dimension tables is used as sortkey;

fact table: DISTKEY & SORTKEY set to be the same join-column so query optimizer/redshift uses a sort merge join 
            instead of a slower hash join;
'''

# CREATE TABLES
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    first_name VARCHAR,
    gender CHAR,
    item_in_session INT,
    last_name VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration BIGINT,
    session_id INT,
    song VARCHAR ,
    status INT,
    ts TIMESTAMP,
    user_agent VARCHAR,
    user_id INT
    );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id VARCHAR,
    artist_location VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,    
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INT
    );
""")


# fact table
songplay_table_create = ("""
CREATE TABLE  IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY SORTKEY DISTKEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL REFERENCES users(user_id),
    level VARCHAR,
    song_id VARCHAR NOT NULL REFERENCES songs(song_id),
    artist_id VARCHAR NOT NULL REFERENCES artists(artist_id),
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR
    );
""")

# dimension table
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY SORTKEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender VARCHAR,
    level VARCHAR NOT NULL
    );
""")

# dimension table
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY SORTKEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL REFERENCES artists(artist_id),
    year INT,
    duration FLOAT
    );
""")

# dimension table
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY SORTKEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
    );
""")

# dimension table
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY SORTKEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
    );
""")


'''
reference to deal with TIMEFORMAT & blank/empty value:
https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html
'''

# STAGING TABLES
staging_events_copy = ("""
COPY staging_events FROM '{}' 
CREDENTIALS 'aws_iam_role={}' 
REGION '{}' 
FORMAT AS JSON '{}' 
TIMEFORMAT AS 'epochmillisecs' 
BLANKSASNULL 
EMPTYASNULL 
TRUNCATECOLUMNS;
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['AWS']['REGION'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs FROM '{}' 
CREDENTIALS 'aws_iam_role={}' 
REGION '{}' 
FORMAT AS JSON 'auto' 
BLANKSASNULL 
EMPTYASNULL 
TRUNCATECOLUMNS;
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'], config['AWS']['REGION'])


# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT DISTINCT s_e.ts, s_e.user_id, s_e.level, s_s.song_id, s_s.artist_id, s_e.session_id, s_e.location, s_e.user_agent
FROM staging_songs AS s_s
JOIN staging_events AS s_e
ON (s_e.song = s_s.title AND s_e.artist = s_s.artist_name)
WHERE s_e.page='NextSong';
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT user_id, first_name, last_name, gender, level
FROM staging_events
WHERE page='NextSong' AND user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT DISTINCT start_time,
EXTRACT(hour FROM start_time) AS hour,
EXTRACT(day FROM start_time) AS day,
EXTRACT(WEEK FROM start_time) AS week,
EXTRACT(month FROM start_time) AS month, EXTRACT(year from start_time) AS year,
EXTRACT(dow FROM start_time) AS dow FROM
    (SELECT DISTINCT ts AS start_time
    FROM staging_events AS s_e
    JOIN staging_songs AS s_s
    ON (s_e.song = s_s.title AND s_e.artist = s_s.artist_name)
    WHERE s_e.page='NextSong');
""")


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create,
                    songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, 
                    songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, 
                    user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
