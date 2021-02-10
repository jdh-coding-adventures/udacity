import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
input_data_path=config['AWS']['INPUT_DATA']
output_data_path=config['AWS']['OUTPUT_DATA']


def create_spark_session():
    """
    Creates a new spark session, or retrieves one if there is already an existing one available.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function extracts data from song data json files to write to song and artist data.
    Song data is partitioned by year and artist id and
    Artist data is not partitioned
    All tables will be overwritten on each execution.
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data,"song_data/A/A/A/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(col("song_id")
                           ,col("title")
                           ,col("artist_id")
                           ,col("year")
                           ,col("duration"))
    
    # write songs table to parquet files partitioned by year and artist
    (songs_table.write
                .partitionBy("year","artist_id")
                .mode("overwrite")
                .format("parquet").save(os.path.join(output_data,"/songs_data/songs_table.parquet")
                                       )
    )
                                                                                                

    # extract columns to create artists table
    artists_table = df.select(col("artist_id")
                             ,col("artist_name").alias("name")
                             ,col("artist_location").alias("location")
                             ,col("artist_latitude").alias("latitude")
                             ,col("artist_longitude").alias("longitude"))
    
    # write artists table to parquet files
    artists_table.write.format("parquet").mode("overwrite").save(os.path.join(output_data,"/artists_data/artists_table.parquet"))


def process_log_data(spark, input_data, output_data):
    """
    This function reads log_data json files and extracts information for users, time and song plays tables.
    Songplays will join back to the songs table - data extracted from the song_data files.
    There is a udf function that converts the epoch time to datetime python format.
    All tables are overwriten with data from dataframes on each execution.
    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(col("page")=="NextSong")

    #extract columns for users table    
    users_table = (df.select(col("userId").alias("user_id")
                             ,col("firstName").alias("first_name")
                             ,col("lastName").alias("last_name")
                             ,col("gender").alias("gender")
                             ,col("level").alias("level")
                           )
                  )   
    # write users table to parquet files
    users_table.write.format("parquet").mode("overwrite").save(os.path.join(output_data,"/users_data/users_table.parquet"))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts/1000))
    df = df.withColumn("timestamp",get_timestamp(col("ts")))
    
    # extract columns to create time table
    time_table = (df.select(date_format(col("timestamp"), "yyyy-MM-dd hh:mm:ss").alias("start_time")
                           ,hour("timestamp").alias("hour") 
                           ,dayofmonth("timestamp").alias("day") 
                           ,weekofyear("timestamp").alias("week")
                           ,month("timestamp").alias("month")
                           ,year("timestamp").alias("year")
                           ,dayofweek("timestamp").alias("weekday")
                          )
                 )
    
    # write time table to parquet files partitioned by year and month
    (time_table.write
               .partitionBy("year","month")
               .format("parquet")
               .mode("overwrite")
               .save(os.path.join(output_data,"/time_data/time_table.parquet"))
    )

    # read in song data to use for songplays table
    song_df = spark.read.format("parquet").load(os.path.join(output_data,"/songs_data/songs_table.parquet"))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = (df.join(song_df, (song_df.title == df.song) &
                                         (song_df.duration == df.length)
                               ,how="left"
                              )
                         .select(monotonically_increasing_id().alias("songplay_id")
                                 ,df.timestamp.alias("start_time").cast(TimestampType())
                                 ,df.userId.alias("user_id")
                                 ,df.level.alias("level")
                                 ,song_df.song_id.alias("song_id")
                                 ,song_df.artist_id.alias("artist_id")
                                 ,df.sessionId.alias("session_id")
                                 ,df.location.alias("location")
                                 ,df.userAgent.alias("userAgent")
                                 ,year(df.timestamp).alias("year")
                                 ,month(df.timestamp).alias("month")
                                )
                       
                      )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy().mode("overwrite").save(os.path.join(output_data,"/songplays_data/songplays_table.parquet"))


def main():
    """
    Main function to call the process_log_data and process_song_data functions.
    """
    
    spark = create_spark_session()
    input_data = input_data_path
    output_data = output_data_path
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
