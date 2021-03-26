from pyspark.sql import SparkSession
from helper_functions import create_spark_session
import pyspark.sql.functions as F
from etl import match_summary_destination_path, chat_destination_path
from analysis_queries import get_total_toxic_matches, get_total_toxic_players, get_most_toxic_region

def main():
    
    #create spark session
    spark = create_spark_session()
    
    chat_match_ids = spark.read.parquet(chat_destination_path).select(F.col("match_id")).distinct()
    match_summary = (spark.read.parquet(match_summary_destination_path)
                                         .select(F.col("toxic"),
                                                 F.col("toxic_player_count"),
                                                 F.col("match_id"),
                                                 F.col("region")
                                         )
    )
    
    match_summary = (match_summary.alias("ms").join(chat_match_ids.alias("chats"),
                                                on=["match_id"],
                                                how="inner"
                                                )
                                            .select("ms.*")
  
    )
    
    #How many macthes were toxic?
    total_matches = get_total_toxic_matches(match_summary=match_summary)
    
    #How many players were toxic?
    get_total_toxic_players(match_summary=match_summary,
                            total_matches=total_matches)
    
    
    #What is the most toxic region?
    get_most_toxic_region(match_summary=match_summary,
                            total_matches=total_matches)
    
    
if __name__ == "__main__":
    
    main()