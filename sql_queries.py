import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS users_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
artist        VARCHAR,
auth          VARCHAR, 
firstName     VARCHAR,
gender        VARCHAR, 
iteminSession INTEGER, 
lastName      VARCHAR, 
length        VARCHAR, 
level         VARCHAR,
location      VARCHAR,
method        VARCHAR,
page          VARCHAR,
registration  VARCHAR, 
sessionId     INTEGER    NOT NULL SORTKEY DISTKEY, 
song          VARCHAR,
status        INTEGER,
ts            BIGINT     NOT NULL, 
userAgent     VARCHAR, 
userId        INTEGER)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs ( 
num_songs        INTEGER,
artist_id        VARCHAR SORTKEY DISTKEY,
artist_latitude  VARCHAR,
artist_longitude VARCHAR, 
artist_location  VARCHAR, 
artist_name      VARCHAR, 
song_id          VARCHAR, 
title            VARCHAR,
duration         FLOAT, 
year             INTEGER)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table (
songplay_id INTEGER    IDENTITY(0,1) PRIMARY KEY SORTKEY,
start_time  TIMESTAMP  NOT NULL,
users_id      INTEGER    NOT NULL DISTKEY,
level        VARCHAR,  
song_id      VARCHAR   NOT NULL,
artist_id    VARCHAR   NOT NULL,
session_id   INTEGER,
location     VARCHAR,
user_agent   VARCHAR);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users_table (
users_id    INTEGER PRIMARY KEY SORTKEY,
first_name VARCHAR,
last_name  VARCHAR,
gender     VARCHAR,
level      VARCHAR) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table(
song_id    VARCHAR  PRIMARY KEY SORTKEY,
title      VARCHAR,
artist_id  VARCHAR,
year       INTEGER,
duration   FLOAT) diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table(
artist_id  VARCHAR PRIMARY KEY SORTKEY,
name       VARCHAR,
location   VARCHAR,
latitude   VARCHAR,
longitude  VARCHAR) diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table(
start_time TIMESTAMP PRIMARY KEY SORTKEY,
hour       INTEGER,
day        INTEGER,
week       INTEGER,
month      INTEGER,
year       INTEGER,
weekday    VARCHAR)diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
json {}
region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
json 'auto'
region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay_table (start_time,
                       users_id,
                       level,
                       song_id,
                       artist_id,
                       session_id,
                       location,
                       user_agent)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'   AS start_time,
            se.userId                   AS users_id,
            se.level                    AS level,
            ss.song_id                  AS song_id,
            ss.artist_id                AS artist_id,
            se.sessionId                AS session_id,
            se.location                 AS location,
            se.userAgent                AS user_agent
    FROM staging_events AS se
    JOIN staging_songs AS ss
        ON (se.artist = ss.artist_name AND se.song = ss.title)
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users_table (users_id,
                   first_name,
                   last_name,
                   gender,
                   level)
    SELECT  DISTINCT se.userId          AS users_id,
            se.firstName                AS first_name,
            se.lastName                 AS last_name,
            se.gender                   AS gender,
            se.level                    AS level
    FROM staging_events AS se
    WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO song_table (song_id,
                   title,
                   artist_id,
                   year,
                   duration)
    SELECT  DISTINCT ss.song_id         AS song_id,
            ss.title                    AS title,
            ss.artist_id                AS artist_id,
            ss.year                     AS year,
            ss.duration                 AS duration
    FROM staging_songs AS ss;
""")

artist_table_insert = ("""
INSERT INTO artist_table (artist_id,
                     name,
                     location,
                     latitude,
                     longitude)
    SELECT  DISTINCT ss.artist_id       AS artist_id,
            ss.artist_name              AS name,
            ss.artist_location          AS location,
            ss.artist_latitude          AS latitude,
            ss.artist_longitude         AS longitude
    FROM staging_songs AS ss;
""")

time_table_insert = ("""
INSERT INTO time_table (start_time,
                  hour,
                  day,
                  week,
                  month,
                  year,
                  weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'        AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM    staging_events                   AS se
    WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
