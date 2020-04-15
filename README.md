## Summary

Wanted to build an ETL pipeline with AWS services like S3, RedShift so got some S3 data related to a music streaming app in the form of JSON log data, profiling user activity, and JSON metadata, describing the songs and artists that are being listened to. 

Built an ETL pipeline to extract the JSON files from Amazon S3 buckets, where they currently reside, and loads them into two staging tables in Amazon Redshift. Later transformed the data and loaded into five tables for analysis, one fact, and four dimension tables, which should help anyone to query the data and make sense of it.


## Instructions

To run the ETL pipeline, follow these steps;

1. Make sure Redshift cluster is running and status is 'healthy'. I have created a Jupyter Notebook (IaaC.ipynb) to do that.
2. Make sure  relevant security policies and IAM role have been provisioned to allow the Redshift cluster to pull data from S3 buckets
3. Create a config file (dwh.cfg) and list the credentials, IAM role ARN, and cluster endpoint in the config file
4. From command line, execute 'python create_tables.py' to create the staging and analytics tables
5. From command line, execute 'python etl.py' to load the staging tables, transform the data, and finally load into the analytics tables from the staging tables
6. Use the AWS Management Console's Redshift Query Editor to query the populated analytics tables or use the Iaac.ipynb to query the tables using the SQL Magic extension.


## ETL

### Staging Tables

Staging tables are used to load data from Amazon S3 buckets into Redshift. They hold the data in its 'raw' format before any transformation has taken place.

#### staging_events

This table holds data from user activity JSON logs from the music streaming app.

The table has the distkey set to the `artist` column so that the events are distributed by the artist name which will make it easier to JOIN both the staging_events and staging_songs tables on `artist`/`artist_name` and `song`/`title` columns to create the songplays table. Distributing by artist in the database will improve the efficiency of the join and improves performance.

#### staging_songs

This table holds JSON-formatted metadata on the songs and artists listened to by users from the music streaming app.

The table has the distkey set to the `artist_name` column so that the events are distributed by the artist name which will make it easier to JOIN both the staging_events and staging_songs tables on `artist`/`artist_name` and `song`/`title` columns to create the songplays table. Distributing by artist in the database will improve the efficiency of the join and improves performance.

### Analytics Tables (Star Schema)

Analytics tables are created by Transforming data and loading the Redshift Data warehouse from the the staging tables.

#### songplays - fact table

Created by joining both of the staging tables, based on the same `artist`/`artist_name` and `song`/`title` columns, to get  data the song listened to, user id, level, location, user agent into one fact table.

This table is distributed by the `artist_id` field and also sorted based on the `artist_id` , and `song_id` so that all the songs related related to one artist and artist information is kept in same slice instead of multiple slices.

Following fieds are extracted from the staging_events table and loaded into the users table.

sessionId    
timestamp     
level     
location     
userAgent     

Following fieds are extracted from the staging_events table and loaded into the users table.

song_id     
artist_id     


#### users - dimension table

Created by using data from the staging_events table.

This table is distributed by the `user_id` field and also sorted based on the `user_id` so that all the activities related to one user is kept in same slice instead of multiple slices.

Following fieds are extracted from the staging_events table and loaded into the users table.

userId     
firstname     
lastname     
gender     
level      


#### songs - dimension table

Created by using data from the staging_songs table.

This table is distributed by the `artist_id` field and also sorted based on the `artist_id` , and `song_id` so that all the songs related related to one artist and artist information is kept in same slice instead of multiple slices.

Following fieds are extracted from the staging_songs table and loaded into the songs table.

song_id     
title     
artist_id      
year     
duration      

#### artists - dimension table

Created by using data from the staging_songs table.

This table is distributed by the `artist_id` field and also sorted based on the `artist_id` so that all the songs related related to one artist and artist information is kept in same slice instead of multiple slices.

Following fieds are extracted from the staging_songs table and loaded into the artists table.

artist_name     
artist_location      
artist_latitude     
artist_longitude     

#### time - dimension table

Created by using the `ts` column from the staging_events table.

The table has as a distribution key, and sorted by `year` 

Timestamp field is extracted from the staging_events table and transformed it to the following fields and loaded into the time table.

time     
hour     
day     
week     
month     
year     
weekday     