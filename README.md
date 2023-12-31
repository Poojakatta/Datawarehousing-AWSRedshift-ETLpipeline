# Datawarehousing-AWSRedshift-ETLpipeline

**Introduction**    
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

**Project Datasets**

We'll be working with 3 datasets that reside in S3.

**Song Dataset**

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset.

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

**Log Dataset**


The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are file paths to two files in this dataset.

![log-data](https://github.com/Poojakatta/Datawarehousing-AWSRedshift-ETLpipeline/assets/63975461/1fe9f87b-845f-447c-8279-22e375bd4a7b)

**Log Json Meta Information**
And below is what data is in log_json_path.json.

![log-json-path](https://github.com/Poojakatta/Datawarehousing-AWSRedshift-ETLpipeline/assets/63975461/72dc800e-662d-4d18-89b9-5ab0a8b62e99)

**Schema for Song Play Analysis**

Using the song and event datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

## Staging modeling

The staging tables that hold the data prior to processing them for OLAP purposes are the following:

**staging_events**

artist_id, auth, first_name, gender, item_in_session, last_name, length, level, location, method, page, registration, session_id, song_title, status, ts, user_agent, user_id

**staging_songs**

num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year

## Fact Table

**songplays** - records in event data associated with song plays i.e. records with page NextSong

_songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_


## Dimension Tables

**users** - users in the app

_user_id, first_name, last_name, gender, level_

**songs** - songs in music database

_song_id, title, artist_id, year, duration_

**artists** - artists in music database

artist_id, name, location, lattitude, longitude

**time** - timestamps of records in songplays broken down into specific units

_start_time, hour, day, week, month, year, weekday_


### The project template includes files:

`create_table.py` - is where you'll create your fact and dimension tables for the star schema in Redshift.

`etl.py` - is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.

`sql_queries.py` - is where you'll define you SQL statements, which will be imported into the two other files above.

`dwh.cfg` - is where you add your configurations varibles.

`AWS environment setup.ipynb` - is where you can create IAM roles, Redshift clusters.

`run.ipynb` - is where we run the program to drop the exisitng and create new tables.

`test.ipynb`- is where we connect to cluster after ETL process is complete and check if the data is loaded properly.


## Steps to run the project

1. Create an IAM role Administrator access and note the key and secret.
2. Set up the AWS Redshift cluster and assign roles so that it can access data from S3 bucket using `AWS environment setup.ipynb` file.
3. Get IAM ARN and cluster end points and fill in the details in `dwh.cfg` file.
4. Run the `run.ipynb` file which first runs `create_tables.py` to create all required tables followed by `etl.py` to load the data into Redshift cluster.
5. Now using `test.ipynb` connect back to cluster and run few commands to test if the data is loaded into cluster correctly.
