## How Toxic is Dota2

### Project Summary
This project analyses Dota 2 match data, chat data and region data to determine how toxic the dota 2 community is.
This project allows someone to process csv files on an ad hoc basis, and will never process the same data twice.
For more information about how the game works, please see the following link:
https://en.wikipedia.org/wiki/Dota_2

1. Scope the Project and Gather Data
2. Run Pipelines
3. Project Files Overview
4. Data Dictionary
5. Project Review

#### 1. Scope the Project and Gather Data
This project will look at dota match data to determine how toxic Dota games are. We will look at 50k matches, 1.3 million chats and 53 regions. At the end we will see:

1. How many macthes were toxic.
2. How many players were toxic.
3. What is the most toxic region.

I will be using spark to read the data into data frames, clean the data where needed, transform the data where needed, and then load the data into a star schema. 
The data is all in csv format and it is not very complex, therefor I will only be using spark for this project. 
The goal is simply to look at a batch of matches and determining the toxicity on that batch(Matches in the match.csv file). 
This project is simply to store data and do analysis over time.

A match will be described as toxic when there is any curse words in the chat, or any griefing chat, so calling the someone a noob or just saying the game was easy.

The data found in this project comes from the Dota 2 Matces data in Kaggle, and can be found at the link below. 
https://www.kaggle.com/devinanzelmo/dota-2-matches

I will add a date created field to all of the above tables to track when new data was added. This will be a timestamp column and will be called date_created in all the tables.


#### 2. Run Pipelines

##### Goal
The goal of this project is to allow a Dota2 enthusiast to use spark to load and analyse a batch of match, chat and region data on an adhoc basis to determine 
how toxic the dota 2 games are. Ideally the destination paths would point to s3, since the history of what has been processed will be kept, and new match data will 
only be added over time. This will allow someone to eventually work through all matches on their own time.

##### Process Flow
Currently I have been running this on my Udacity workspace, and the process below is a simple 3 step process, since I am only using 1 set of chat, match and region data.
All of these commands will be run from the terminal.

1. If you need to start from the beginning and the data already exists run the following 2 commands to create an empty destination forlder:
    - rm -r dota2
    - mkdir dota2

2. Run the etl.python script. This will do all the etl work to get the data from csv into parquet format and create the match summary table.
    - python etl.py

3. Run the analysis.py script. This script will analyse the match summary data to determine how many games were toxic, how many players were toxic, and which
    region was the most toxic. These amounts will be printed to screen in percentages and will not be stored anywhere. That is why this can be run seperately from the
    etl process, because this will allow the person to run this at any time without having to load new data.
    - python analysis.py


#### 3. Project Files Overview

The project consists of 5 python files, 3 of them are helper files and 2 of them will be run in the command line.

**etl.py**
This is the main file and contains the etl process. In this file script file we run the following functions to run the etl process and do the data quality checks.

1. process_chat_data
    Here we read the chat data from csv to a dataframe, add the current date and time as a for the date_processed column, and write the data to parquet files
    partitioned by the process date. This will only add new data, and will never overwrite existing data or process the same data twice.

    Args:
        - chat_source_path: This is a *string* pointing to the csv for the chat data.
        - chat_destination_path: This is a *string* pointing to the destination where the chat parquet files will be written to.
        - spark: This is the *SparkSession* created by the create_spark_session function.

2. process_region_data
    Here we read the chat data from csv to a dataframe, and always overwrite the parquet file. This file is small so can always be processed as a full file.

    Args:
        - region_source_path: This is a *string* pointing to the csv for the region data.
        - region_destination_path: This is a *string* pointing to the destination where the region parquet files will be written to.
        - spark: This is the *SparkSession* created by the create_spark_session function.


3. process_match_data
    Here we read the match data from csv to a dataframe, add the current date and time as a for the date_processed column, and write the data to parquet files
    partitioned by the process date. This will only add new data, and will never overwrite existing data or process the same data twice.

    Args:
        - match_source_path: This is a *string* pointing to the csv for the match data.
        - match_destination_path: This is a *string* pointing to the destination where the match parquet files will be written to.
        - spark: This is the *SparkSession* created by the create_spark_session function.

4. get_record_counts
    Here we get the row counts from the above tables for the current etl run.

    Args:
        - destination_table_paths: This is a *list* containing the destination
        - spark: This is the *SparkSession* created by the create_spark_session function.
        - date_processed: This is a python *datetime* value generated at the start. This is to ensure that the current etl run loads data.

5. get_orphaned_records
    Here we check that there are no orphaned records. We pass in a source and destination path, and we check if there are any records from the source that do not exist in the destination. This is to ensure data integrity, since we cannot enforce referential integrity.
    We use this one function to check all source and destinations.

    Args:
        - source_destination_path: This is a *string* pointing to the source path which represents the foreign key value in a relational database.
        - target_destination_path: This is a *string* pointing to the destination path which represents the primary key value in a relational database.
        - source_column_name: This is a *string* pointing the the join column from the source table.
        - target_column_name: This is a *string* pointing the the join column from the destination table.
        - spark: This is the *SparkSession* created by the create_spark_session function.


6. process_match_summary
    Here we use the match, chat and region data to create a summary table containing the data for each match. 
        - We get the match_id, match_date and cluster_id from the match data.
        - We the match_id, chat, player_name from the chat data. We also pass the chat column to a function that determines weather the chat contains bad words.
        - We get the entire region dataset.
        - We combine all of the above and run a window function over it where we partition by match_id, and we order by match_id and the toxic column. This allows us
          to do the aggregations required.
        - Then we get the dataframe in the same format as the destination and writes the data to parquet, partitioning by match_date.

    Args:
        - match_destination_path: This is a *string* pointing to processed match data.
        - chat_destination_path: This is a *string* pointing to processed chat data.
        - region_destination_path: This is a *string* pointing to processed region data.
        - match_summary_destination_path: This is a *string* pointing to where the match_summary_data parquet files will be written to.
        - spark: This is the *SparkSession* created by the create_spark_session function.

There are helper functions used in these functions which are stored in the helper_function.py file and the dq checks are stored in the data_quality_functions.py

**helper_functions.py**
This is a helper script for the etl.py script.

1. create_spark_session
    Creates a new spark session.

    Args:
        - No args

2. process_data_frame
    Processes a dataframe. This is used for the dataframes that contains the data from the csv files.

    Args:
        - destination_file_path: This is a *string to the destination where the parquet files will be written to.
        - join_column_name: This is a *string and is the name of the join column. The join column is used when new data is processed, to exclude existing data before 
                            writing the dataframe to parquet.
        - partition_column_name: This is a *string and is the name of the column that the parquet files will be partitioned by.
        - spark: This is the *SparkSession created be the previous function.
        - renamed_df: This is a *dataframe containing data from the csv files.

3. get_toxic
    This function is used to determine if a chat is toxic or not. It contains a list of pre-defined bad words, and then loops through them for each chat. It will 
    check the chat to see if any word is in the chat. This is a wild card operation and not an exact match. For example if the word it is looking for is walk, and the chat is 
    "I am walking at the moment", the chat would still be flagged as toxic. It returns the length of the toxic word.

    Args:
        - chat: This is a *string*, and comes from a free text field of whatever was typed into the chat window in the game.

**data_quality_functions.py**

This is a helper script for the etl.py script. This is one of the helper scripts, and they are documented in the etl.py at number 4 and 5 respectively.

1. get_record_counts
    Here we get the row counts from the above tables for the current etl run.

    Args:
        - destination_table_paths: This is a *list* containing the destination
        - spark: This is the *SparkSession* created by the create_spark_session function.
        - date_processed: This is a python *datetime* value generated at the start. This is to ensure that the current etl run loads data.

2. get_orphaned_records
    Here we check that there are no orphaned records. We pass in a source and destination path, and we check if there are any records from the source that do not exist 
    in the destination. This is to ensure data integrity, since we cannot enforce referential integrity.
    We use this one function to check all source and destinations.

    Args:
        - source_destination_path: This is a *string* pointing to the source path which represents the foreign key value in a relational database.
        - target_destination_path: This is a *string* pointing to the destination path which represents the primary key value in a relational database.
        - source_column_name: This is a *string* pointing the the join column from the source table.
        - target_column_name: This is a *string* pointing the the join column from the destination table.
        - spark: This is the *SparkSession* created by the create_spark_session function.

**analysis.py**
This is the main script file for the analytical queries. It will execute each query in the analysis_queries.py helper script.

1. create_spark_session
    Creates a new spark session.

    Args:
        - No args

2. Reads match_ids from the chat data into a dataframe.

3. Reads match_ids from match_summary data

4. Gets only the match ids from match_summary if we also have chat data for them, and we do the analysis only on matches that we have chat data for.

5. get_total_toxic_matches
    Get the total number of matches from the dataframe, then the amount of toxic matches,
    and then get the percentage of how many were toxic. This returns the tota match count, which will be used by the other queries.

    Args:
        - match_summary: This is the *dataframe* containing match summary data that is already in a reporting format.

6. get_total_toxic_players
    Calculate the total players by multiplaying the amount of matches by 10, because there are 10
    players in a match.
    Get the amount of toxic players by doing a sum on the toxic player count. This was calculated by counting
    the distinct number of players who was toxic per game.
    Get the percentage of toxic players across all the games.

    Args:
        - match_summary: This is the *dataframe* containing match summary data that is already in a reporting format.
        - total_matches: This is an *integer* returned by the first query. It indicates the total amount of matches that are in the dataframe.

7. get_most_toxic_region
    Get a count of how many time each region appears in the toxic matches. We order this by the ttal amount in descending order,
    so that the most frequent region is at the top, and we take only the first result.

    Args:
        - match_summary: This is the *dataframe* containing match summary data that is already in a reporting format.
        - total_matches: This is an *integer* returned by the first query. It indicates the total amount of matches that are in the dataframe.
    

**analysis_queries.py**
This is the helper script that contains the definition for each query that is executed in the analysis.py script. 
These functions are explained in the previous section in numbers 5,6 and 7 respectively.

1. get_total_toxic_matches
    Get the total number of matches from the dataframe, then the amount of toxic matches,
    and then get the percentage of how many were toxic. This returns the tota match count, which will be used by the other queries.

    Args:
        - match_summary: This is the *dataframe* containing match summary data that is already in a reporting format.

2. get_total_toxic_players
    Calculate the total players by multiplaying the amount of matches by 10, because there are 10
    players in a match.
    Get the amount of toxic players by doing a sum on the toxic player count. This was calculated by counting
    the distinct number of players who was toxic per game.
    Get the percentage of toxic players across all the games.

    Args:
        - match_summary: This is the *dataframe* containing match summary data that is already in a reporting format.
        - total_matches: This is an *integer* returned by the first query. It indicates the total amount of matches that are in the dataframe.

3. get_most_toxic_region
    Get a count of how many time each region appears in the toxic matches. We order this by the ttal amount in descending order,
    so that the most frequent region is at the top, and we take only the first result.

    Args:
        - match_summary: This is the *dataframe* containing match summary data that is already in a reporting format.
        - total_matches: This is an *integer* returned by the first query. It indicates the total amount of matches that are in the dataframe.

#### 4. Data Dictionary

**chat.csv**

1. match_id - This is just a bigint and uniquely identifies the match
2. key - This is the actual chat string. This will be renamed to chat
3. slot - This is the position of slot for the player, it can be 0 - 9 since there are 10 players in a game.
4. time - This is the time in seconds into the game, and will be renamed to time_in_seconds
5. unit - This is the name of the player, and it will be renamed to player

**cluster_region.csv**

1. cluster - This is an integer column, and is it is the cluster id. This will be renamed to cluster_id
2. region - This is the name of the region.

**match.csv**

1. match_id - This is just a bigint and uniquely identifies the match
2. start_time - This is a big int, and it is a unix timestamp. This will be converted to a spark timestamp.
3. duration - This is an int field, and shows the duration of the match in seconds, and will be renamed to duration_in_seconds
4. tower_status_radiant - A particular teams tower status is given as a 16-bit unsigned integer. The rightmost 11 bits represent individual towers belonging to that team; see below for a visual representation.
5. tower_status_dire - A particular teams tower status is given as a 16-bit unsigned integer. The rightmost 11 bits represent individual towers belonging to that team; see below for a visual representation.
6. barracks_status_dire - A particular teams tower status is given as an 8-bit unsigned integer. The rightmost 6 bits represent the barracks belonging to that team; see below for a visual representation.
7. barracks_status_radiant - A particular teams tower status is given as an 8-bit unsigned integer. The rightmost 6 bits represent the barracks belonging to that team; see below for a visual representation.
8. first_blood_time - Time in seconds of when the first hero died in a match
9. game_mode - The mode the game is played e.g All pick, Turbo, Random Draft etc
10. radiant_win - Boolean value to say which side won the match
11. negative_votes - The number of thumbs-down the game has received by users
12. positive_votes - The number of thumbs-up the game has received by users
13. cluster - The cluster id the game was played on. This will be renamed to cluster id

A link for a more in depth description of match data - https://wiki.teamfortress.com/wiki/WebAPI/GetMatchDetails#Tower_Status

**match_summary**
match_id - This is just a bigint and uniquely identifies the match
match_date - Date of when the match took place
toxic - Boolean value indicating if a match was toxic or not
toxic_count - Integer indicating how many times there were toxic comments in the match
toxic_player_count - Integer indicating how many players were toxic in a match
region - Name of the region where the game took place.


#### 5. Project Review

##### Choice of technologies
For this project I am only using spark on my udacity workspace. The reason for spark is that it is the best tool to use when reading in data from csv,
and processing big amounts of data, and it can also be used for the storing and analysis of the data. 
So essentially I chose to use only spark because I could achive the goals I set out from the start. 
I will propably be working on this long after this course so I will be moving data to an s3 bucket since I will not have access to the udacity workspace anymore.

##### How often the data should be updated
I designed this project so the user has the luxury of loading data when it suits him on an adhoc basis. This can be done daily, monthly or even in a year from now,
as long as the source csv files are in the same format.

##### Different approaches for different scenarios

###### The data was increased by 100x
If the initial data was 100 times bigger then I would probably have used airflow to schedule loads based on the match date to back fill the data in parallel. 
In that case this would not have allowed the used to just run thhis process on an adhoc basis. I would also have used a big EMR cluster in the amazon 
environment and stored the data in s3 from the start, or potentially even in Redshift.

###### The data populates a dashboard that must be updated on a daily basis by 7am every day
If the data needed to be loaded be 7 am every day I would use airflow to schedule the data load. I would also not be able to get the data the current source which is kaggle. 
In that case I would have to hit the dota api directly maybe just after midnight, and then export it to s3. 
Then I would add an s3 trigger to start the airflow dag once all the data has been exported into csv files on s3.

###### The database needed to be accessed by 100+ people
In this particular scope of the project there would not be this amount of people needing to access the data. 
But lets say we broaden the scope where we provide a central storage loacation with all the data and allow actual players to access their match data 
to not only determine the toxicity of their matches, but also a bunch of other performance metrics on their matches, 
then I would potentially store the data in s3 as a storage layer, and then use spark to analyse the data. 
An easy way to achieve this is to use databricks. Another option would be to use Redshift as the data store as previously mentioned, 
in this case we would need to configure the Redshift cluster to automatically scale to handle the concurrency of the workload. 

For me personally I would use databricks, since I have grown to love spark, and it is very flexible 
and it allows you to access and manipulate the data using multiple languages such as SQL, scala, Python(pyspark, pandas etc) and spark.