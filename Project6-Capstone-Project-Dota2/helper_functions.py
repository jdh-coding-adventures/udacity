from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from toxic_words import toxic_words_list

def create_spark_session():
    """
    This function creaes a new spark session. 
    """
    spark = (SparkSession.builder.
        config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")
        .enableHiveSupport().getOrCreate()
    )

    return spark

def process_data_frame(destination_file_path: str,
                        join_column_name: str,
                        partition_column_name: str,
                        spark: SparkSession,
                        renamed_df: DataFrame):
    """
    This function is used to process the csv files
    It first checks if the destination already exists. If not it will create it, and if it does exist, it will just append the new data.
    No data will be overwritten, so it is safe to run this function as many times with the same data as you want.
    """
    
    destination_data = destination_file_path.split("/")[-1]
    
    try:
        existing_data = spark.read.parquet(destination_file_path).select(F.col(join_column_name)).distinct()
    except:
        existing_data = None
        
    if existing_data and existing_data is not None:
        
        renamed_df = (renamed_df.alias("new")
                                .join(existing_data.alias("cur"),
                                        on=["match_id"],
                                        how="left_anti"
                                    )
                                .select("new.*")
                     )
    
        print("Processing " + str(renamed_df.count()) + f" new rows for {destination_data}.")
        renamed_df.write.format("parquet").mode("append").partitionBy(partition_column_name).save(destination_file_path)
              
    else:
        print(f"Processing complete csv file for {destination_data}, " + str(renamed_df.count()) + " rows.")
        renamed_df.write.format("parquet").mode("overwrite").partitionBy(partition_column_name).save(destination_file_path)
        
        
@udf()
def get_toxic(chat):
    """
    This function determines if a chat is toxic or not.
    It uses the list of bad words below, loops through it and checks if any of them exist in the chat.add()
    It does not require an exact match to flag the word, for example it will flag arsehole because arse is in the list.
    """
    if chat is None:
        chat = "g"
    flag_words = toxic_words_list
    toxic = [flag_word for flag_word in flag_words if(flag_word in chat)]
    return len(toxic)