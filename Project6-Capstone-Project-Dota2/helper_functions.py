from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import udf

def create_spark_session():

    spark = (SparkSession.builder.
        config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")
        .enableHiveSupport().getOrCreate()
    )
    #df_spark =spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')

    return spark

def process_data_frame(destination_file_path: str,
                                join_column_name: str,
                                partition_column_name: str,
                                spark: SparkSession,
                                renamed_df: DataFrame):
    
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
    
        print("Processing " + str(renamed_df.count()) + " new rows.")
        renamed_df.write.format("parquet").mode("append").partitionBy(partition_column_name).save(destination_file_path)
              
    else:
        print("Processing complete csv file, " + str(renamed_df.count()) + " rows.")
        renamed_df.write.format("parquet").mode("overwrite").partitionBy(partition_column_name).save(destination_file_path)
        
        

@udf()
def get_toxic(chat):
    if chat is None:
        chat = "g"
    flag_words = (["fuck","shit","piss","dick","ass","arse","bitch","bastard","cunt","shag","wank"
                   ,"toss","vagi","twat","root","bugger","ez","noob","moron","retard","idiot","poes"
                  ,"doos","kont","donner","moer","bliksem"]
                 )
    toxic = [flag_word for flag_word in flag_words if(flag_word in chat)]
    return len(toxic)