<h4>Puspose of the datawarehouse</h4>

The purpose of the data warehouse is to analyse data that was logged in terms of songs that were streamed in each session per user. It is very important for a startup to understand how users are using their app in order to make good business decisions to scale the business.

Startups will usually have KPI's to track their growth, and to see where their shortfalls are. The dataware house is used to measure performance in terms of KPI's, and to predict trends in the data.


<h4>Database design and etl pipe line</h4>

# ETL Pipeline

There are 2 staging tables that contain song data and log data. These tables are loaded from json files which are stored in s3.
The staging_songs table contains data about songs and artists.
The staging_events has all event data that was recorded from the app. We will filter on the page column for only events which are "NextSong" since we only want to analyse which songs were streamed.

All dimension tables as well as the fact table are loaded from these two staging tables.


# Database design
The songplays table(fact table) is distributed according to the artist id and sorted in each slice according to the start_time.
The songs and artists tables are both distributed using the artist id, this matches the fact table distribution and will decrease the chance of shuffling.
The users dimension table is not very big and therefore is copied entirely accross each slice.
The time table will be created according to what redshift decides is the best way by the data in the table.

The below is a list of the tables as well as their distribution keys and sort keys.

Fact table 
songplays 
start_time > sort key
artist_id > dist key

Dimension tables 

users 
Distributed on all slices

songs
artist_id > dist key

artists
artist_id > dist key and sort key

time
start_time > sort key
Distribution will be decided by Redshift



<h4>Analytical Queries</h4>

# How many unique users streamed in a specific month

    
SELECT COUNT (DISTINCT user_id)
FROM songplays 
WHERE start_time >= '2018-11-01 00:00:00'
AND start_time < '2018-12-01 00:00:00'
   
 
# 5 Most popular straming locations

SELECT location, COUNT(*) Streams
FROM songplays 
WHERE start_time >= '2018-11-01 00:00:00'
AND start_time < '2018-12-01 00:00:00'
GROUP BY location
ORDER BY Streams DESC
LIMIT 5

# Most streamed artist in each of the  previous popular locations and the amount of streams for the artists

SELECT sp.location,a.name, COUNT(*) Streams
FROM songplays sp
INNER JOIN artists a ON a.artist_id = sp.artist_id
WHERE start_time >= '2018-11-01 00:00:00'
AND start_time < '2018-12-01 00:00:00'
GROUP BY sp.location,a.name
ORDER BY Streams DESC
LIMIT 5