import pandas as pd
from pyspark.sql.types import BooleanType, LongType, IntegerType, ShortType, TimestampType, StringType, DateType
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from datetime import datetime
from helper_functions import create_spark_session, process_data_frame, get_toxic
from data_quality_functions import get_record_counts, get_orphaned_records
from pyspark.sql.window import Window

#define location variables
chat_source_path = "./chat.csv"
chat_destination_path = "./dota2/chat_data"
match_source_path = "./match.csv"
match_destination_path = "./dota2/match_data"
region_source_path = "./cluster_regions.csv"
region_destination_path = "./dota2/cluster_regions_data"
match_summary_destination_path = "./dota2/match_summary_data"

#current datetime
date_processed = datetime.now()


def process_chat_data(chat_source_path:str 
                      ,chat_destination_path: str
                      ,spark: SparkSession):
    """
    This function will process chat data.
    The chat data source is in csv format, and will be written out as parquet format.
    If the destination files already exists, the current data will be appended, and existing data will not be duplicated or overwritten, if not the files will be created.
    """
    
    try:
    
        chat_df = spark.read.csv(header=True,inferSchema=True,path=chat_source_path)
        
        rename_chat_df = (chat_df.select(F.col("match_id").cast(LongType()),
                                    F.col("key").alias("chat").cast(StringType()),
                                    F.col("slot").cast(ShortType()),
                                    F.col("time").alias("time_in_seconds").cast(IntegerType()),
                                    F.col("unit").alias("player_name").cast(StringType())
                                    )
                            .withColumn("date_processed",F.lit(date_processed))
                            .dropDuplicates()
        )
        
        process_data_frame(destination_file_path=chat_destination_path,
                            join_column_name="match_id",
                            partition_column_name="date_processed",
                            spark=spark,
                            renamed_df=rename_chat_df)
        
    except Exception as e:
        print(e)
        

def process_region_data(region_source_path: str,
                        region_destination_path: str,
                        spark: SparkSession):
    """
    This function will process chat data.
    The region data source is in csv format, and will be written out as parquet format.
    Since there is not a lot of region data, this will always perform a full overwrite of the table.
    """
    
    try:
        
        regions_df = spark.read.csv(header=True,inferSchema=True,path=region_source_path)
        
        rename_region_df = (regions_df.select(F.col("cluster").alias("cluster_id").cast(IntegerType()),
                                        F.col("region").cast(StringType())
                                        )
                                .withColumn("date_processed",F.lit(date_processed))
                                .dropDuplicates()
                                .repartition(1)
                    )

        rename_region_df.write.format("parquet").mode("overwrite").save(region_destination_path)
        print("Processing full file containing " + str(rename_region_df.count()) + " rows.")
        
    except Exception as e:
        print(e)
    

def process_match_data(match_source_path: str,
                       match_destination_path: str,
                       spark: SparkSession):
    
    """
    This function will process match data.
    The match data source is in csv format, and will be written out as parquet format.
    If the destination files already exists, the current data will be appended, and existing data will not be duplicated or overwritten, if not the files will be created.
    """
    
    try:
        match_df = spark.read.csv(header=True,inferSchema=True,path=match_source_path)
        
        rename_match_df = (match_df.select(F.col("match_id").cast(LongType()),
                                   F.col("start_time").cast(TimestampType()),
                                   F.col("start_time").alias("match_date").cast(TimestampType()).cast(DateType()),
                                   F.col("duration").alias("duration_in_seconds").cast(IntegerType()),
                                   F.col("tower_status_radiant").cast(IntegerType()),
                                   F.col("tower_status_dire").cast(IntegerType()),
                                   F.col("barracks_status_dire").cast(IntegerType()),
                                   F.col("barracks_status_radiant").cast(IntegerType()),
                                   F.col("first_blood_time").cast(IntegerType()),
                                   F.col("game_mode").cast(ShortType()),
                                   F.col("radiant_win").cast(BooleanType()),
                                   F.col("negative_votes").cast(ShortType()),
                                   F.col("positive_votes").cast(ShortType()),
                                   F.col("cluster").alias("cluster_id").cast(IntegerType())
                                  )
                            .withColumn("date_processed",F.lit(date_processed))
                            .dropDuplicates()
        )
        
        process_data_frame(destination_file_path=match_destination_path,
                            join_column_name="match_id",
                            partition_column_name="match_date",
                            spark=spark,
                            renamed_df=rename_match_df)
        
    except Exception as e:
        print(e)
        raise


def process_match_summary(match_destination_path:str,
                        chat_destination_path: str,
                        region_destination_path: str,
                        match_summary_destination_path: str,
                        spark: SparkSession
                          ):
    """
    This function uses data from the match, chat and region data that have already been written to parquet format.
    In here we read only the columns from each data set, combine them into one and the use a window function to get the aggregate details to enable us to answer
    questions we wanted to answer about how toxic the environment is:
        - We get a distinct count of matches that contain toxic language to see how many games are toxic.
        - We get a total count of toxic comments per metch.
        - We get the total count of toxic players per game.
        - We get the total toxic games per region.
    """
    
    try:
    
        #get required data from match destination
        match_data_df = (spark.read.parquet(match_destination_path)
                                .select(F.col("match_id"),
                                        F.col("match_date"),
                                        F.col("cluster_id")
                                    )                                  
        )
                        
        #get required data from chat destination
        chat_data = (spark.read.parquet(chat_destination_path)
                            .select(F.col("match_id"),
                                    F.col("chat"),
                                    F.col("player_name")
                                )
                                .withColumn("toxic",F.when(get_toxic(F.col("chat"))>0,1)
                                                    .otherwise(0)
                                )
        )

        #get required data from region destination
        region_data = (spark.read.parquet(region_destination_path))

        #combine all data sets
        combined_df = (match_data_df.alias("match").join(chat_data.alias("chat"),
                                                        on=["match_id"],
                                                        how="inner"
                                                        )
                                                .join(region_data.alias("region"),
                                                        on=["cluster_id"],
                                                        how="inner"
                                                        )
                                                    
                    )

        #create partitioning specification for window function
        windowSpec = (Window.partitionBy(F.col("match_id"))
                        .orderBy(F.col("match_id")
                                ,F.col("toxic").desc()
                        )
        )
        
        #use window function to get aggregated information required
        match_summary_df = (combined_df.groupBy("match_id","match_date","toxic","region")       
                        .agg(F.count("match_id").alias("toxic_count"),
                            F.countDistinct("player_name").alias("toxic_player_count")
                        )
                        .withColumn("rn",F.row_number().over(windowSpec))
                        .sort("match_id")
        )
        
        #get dataframe in correct format in order to write to parquet format
        match_summary_final_df = (match_summary_df.filter(F.col("rn")==1)
                                            .select(F.col("match_id").cast(LongType()),
                                                    F.col("match_date").cast(DateType()),
                                                    F.col("toxic").cast(BooleanType()),
                                                    F.col("toxic_count").cast(IntegerType()),
                                                    F.col("toxic_player_count").cast(IntegerType()),
                                                    F.col("region")
                                                    )
                            
        )
        
        #write dataframe to parquet
        process_data_frame(destination_file_path=match_summary_destination_path,
                        join_column_name="match_id",
                        partition_column_name="match_date",
                        spark=spark,
                        renamed_df=match_summary_final_df)
    except Exception as e:
        print(e)
        raise

def main():
    
    """
    This is the main function that will run the etl process as well as the data quality checks.
    """
    
    #create spark session
    spark = create_spark_session()
    
    ##########################
    ## PROCESS SOURCE FILES ##
    ##########################
    
    #process chat data
    process_chat_data(chat_source_path=chat_source_path 
                      ,chat_destination_path=chat_destination_path
                      ,spark=spark)
    
    
    #process region data
    process_region_data(region_source_path=region_source_path,
                        region_destination_path=region_destination_path,
                        spark=spark)
    
    #process match data
    process_match_data(match_source_path=match_source_path,
                       match_destination_path=match_destination_path,
                       spark=spark)
    
    
    ##############
    ## DQ CHECK ##
    ##############
    
    #list of destination tables to check row counts
    destination_table_paths = [chat_destination_path, match_destination_path, region_destination_path]
    
    get_record_counts(destination_table_paths=destination_table_paths,
                      spark=spark,
                      date_processed=date_processed)
    
    #check for chats where we do not have the match data
    get_orphaned_records(source_destination_path=chat_destination_path,
                                target_destination_path=match_destination_path,
                                source_column_name="match_id",
                                target_column_name="match_id",
                                spark=spark)
    
    
    #check for regions in the match data that do not exist in the region data
    get_orphaned_records(source_destination_path=match_destination_path,
                                target_destination_path=region_destination_path,
                                source_column_name="cluster_id",
                                target_column_name="cluster_id",
                                spark=spark)
        
    
    #check for matches that we do not have chats for
    get_orphaned_records(source_destination_path=match_destination_path,
                                target_destination_path=chat_destination_path,
                                source_column_name="match_id",
                                target_column_name="match_id",
                                spark=spark)
    
    ########################
    ## LOAD MATCH SUMMARY ##
    ########################
    
    process_match_summary(match_destination_path=match_destination_path,
                        chat_destination_path=chat_destination_path,
                        region_destination_path=region_destination_path,
                        match_summary_destination_path=match_summary_destination_path,
                        spark=spark
                          )
    

if __name__ == "__main__":
    
    main()
    