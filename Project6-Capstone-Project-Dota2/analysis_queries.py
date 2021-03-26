from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def get_total_toxic_matches(match_summary: DataFrame):
    
    
    total_matches = match_summary.count()
    total_toxic_matches =  match_summary.filter(F.col("toxic")==True).count()
    print(str(round((total_toxic_matches/total_matches*100),2)) + "% of matches were toxic!" )
    
    return total_matches
    
    
def get_total_toxic_players(match_summary: DataFrame,
                            total_matches:int
                            ):
    
    total_players = total_matches * 10
    total_toxic_players = match_summary.filter(F.col("toxic")==True).agg(F.sum("toxic_player_count")).collect()[0][0]
    print(str(round((total_toxic_players/total_players*100),2)) + "% of players were toxic!")
    
def get_most_toxic_region(match_summary: DataFrame,
                          total_matches: int):
    
    most_toxic_region = match_summary.filter(F.col("toxic")==True).groupBy("region").count().sort(F.col("count").desc()).limit(1)
    
    print(most_toxic_region.select("region").collect()[0][0] + " is the most toxic region with " + 
      str(round((most_toxic_region.select("count").collect()[0][0]/total_matches*100),2)) + "% of the toxic matches.")