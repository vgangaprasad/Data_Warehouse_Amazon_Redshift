import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

#Staging tables
staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events (
artist VARCHAR ENCODE ZSTD,
auth VARCHAR ENCODE ZSTD,
firstName VARCHAR ENCODE ZSTD,
gender CHAR ENCODE BYTEDICT,
itemInSession INT ENCODE DELTA,
lastName VARCHAR ENCODE ZSTD,
length DECIMAL ENCODE DELTA,
level VARCHAR ENCODE ZSTD,
location VARCHAR ENCODE ZSTD,
method VARCHAR ENCODE ZSTD,
page VARCHAR ENCODE ZSTD,
registration DECIMAL ENCODE DELTA,
sessionId INT ENCODE DELTA,
song VARCHAR ENCODE ZSTD,
status VARCHAR ENCODE ZSTD,
ts VARCHAR ENCODE ZSTD,
userAgent VARCHAR ENCODE ZSTD,
userId INT ENCODE DELTA
)
DISTKEY(artist)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
artist_id VARCHAR ENCODE ZSTD,
artist_latitude DECIMAL ENCODE DELTA, 
artist_location VARCHAR ENCODE ZSTD,
artist_longitude DECIMAL ENCODE DELTA,
artist_name VARCHAR ENCODE ZSTD, 
duration DECIMAL ENCODE BYTEDICT, 
num_songs INT ENCODE BYTEDICT, 
song_id VARCHAR ENCODE ZSTD,
title VARCHAR ENCODE ZSTD,  
year INT ENCODE DELTA)
DISTKEY(artist_name)
""")

# Fact table
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
songplay_id INT IDENTITY(0,1) ENCODE DELTA, 
start_time TIMESTAMP NOT NULL ENCODE DELTA,
user_id INT NOT NULL ENCODE DELTA,
level VARCHAR ENCODE RAW,
song_id VARCHAR ENCODE ZSTD,
artist_id VARCHAR ENCODE ZSTD,
session_id INT ENCODE DELTA,
location VARCHAR ENCODE ZSTD,
user_agent VARCHAR ENCODE ZSTD)
DISTKEY(artist_id)
COMPOUND SORTKEY (artist_id, song_id)
""")

#Dimension tables
user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
user_id INT PRIMARY KEY ENCODE DELTA, 
first_name VARCHAR ENCODE ZSTD,
last_name VARCHAR ENCODE ZSTD,
gender CHAR ENCODE BYTEDICT,
level VARCHAR(25) ENCODE ZSTD)
DISTKEY(user_id)
COMPOUND SORTKEY (user_id)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
song_id VARCHAR NOT NULL ENCODE ZSTD,
title VARCHAR ENCODE ZSTD,
artist_id VARCHAR NOT NULL ENCODE ZSTD,
year INT ENCODE BYTEDICT,
duration DECIMAL ENCODE BYTEDICT)
DISTKEY(artist_id)
COMPOUND SORTKEY (artist_id, song_id)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
artist_id VARCHAR PRIMARY KEY ENCODE ZSTD,
name VARCHAR NOT NULL ENCODE ZSTD,
location VARCHAR ENCODE ZSTD,
latitude DECIMAL ENCODE DELTA,
longitude DECIMAL ENCODE DELTA)
DISTKEY(artist_id)
SORTKEY(artist_id)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
time TIMESTAMP PRIMARY KEY ENCODE DELTA,
hour INT ENCODE BYTEDICT,
day INT  ENCODE BYTEDICT,
week INT ENCODE BYTEDICT,
month INT ENCODE BYTEDICT,
year INT ENCODE BYTEDICT,
weekday INT ENCODE BYTEDICT)
DISTKEY(YEAR)
SORTKEY(YEAR)
""")

# STAGING TABLES COPY
staging_events_copy = ("""COPY staging_events FROM {s3_bucket} 
credentials 'aws_iam_role={arn}' 
region 'us-west-2' 
FORMAT AS JSON {json_path};
""").format(s3_bucket = config['S3']['LOG_DATA'],
            arn = config['IAM_ROLE']['ARN'],
            json_path = config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""COPY staging_songs FROM {s3_bucket} 
credentials 'aws_iam_role={arn}' 
region 'us-west-2' 
FORMAT AS JSON 'auto';
""").format(s3_bucket = config['S3']['SONG_DATA'],
            arn = config['IAM_ROLE']['ARN'])


# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(session_id, start_time, user_id, level, song_id, artist_id, location, user_agent)
SELECT DISTINCT se.sessionId, (TIMESTAMP WITH TIME ZONE 'epoch' + se.ts/1000 * INTERVAL '1 Second '),
se.userId, 
se.level, 
ss.song_id, 
ss.artist_id, 
se.location, 
se.userAgent
FROM staging_events se 
JOIN staging_songs ss 
ON se.artist = ss.artist_name 
AND se.song = ss.title
WHERE se.page = 'NextSong'
""")

user_table_insert = ("""INSERT INTO users(
SELECT 
DISTINCT userId,
firstname,
lastname,
gender,
level 
FROM staging_events
WHERE userId is NOT NULL)
""")

song_table_insert = ("""INSERT INTO songs(
SELECT 
DISTINCT song_id, 
title, 
artist_id, 
year, 
duration 
FROM staging_songs
WHERE song_id IS NOT NULL
AND artist_id IS NOT NULL)
""")

artist_table_insert = ("""INSERT INTO artists(
SELECT DISTINCT artist_id, 
artist_name, 
artist_location, 
artist_latitude, 
artist_longitude 
FROM staging_songs
WHERE artist_id IS NOT NULL)
""")


time_table_insert = ("""INSERT INTO time(
SELECT DISTINCT 
(TIMESTAMP WITH TIME ZONE 'epoch' + ts/1000 * INTERVAL '1 Second '),
extract(hour from (TIMESTAMP WITH TIME ZONE 'epoch' + ts/1000 * INTERVAL '1 Second ')),
extract(day from (TIMESTAMP WITH TIME ZONE 'epoch' + ts/1000 * INTERVAL '1 Second ')),
extract(week from (TIMESTAMP WITH TIME ZONE 'epoch' + ts/1000 * INTERVAL '1 Second ')),
extract(month from (TIMESTAMP WITH TIME ZONE 'epoch' + ts/1000 * INTERVAL '1 Second ')),
extract(year from (TIMESTAMP WITH TIME ZONE 'epoch' + ts/1000 * INTERVAL '1 Second ')),
extract(dow from (TIMESTAMP WITH TIME ZONE 'epoch' + ts/1000 * INTERVAL '1 Second '))
FROM staging_events)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
