from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def get_total_toxic_matches(match_summary: DataFrame):
    """
    Get the total number of matches from the dataframe, then the amount of toxic matches,
    and then get the percentage of how many were toxic.
    """
    
    
    total_matches = match_summary.count()
    total_toxic_matches =  match_summary.filter(F.col("toxic")==True).count()
    print(str(round((total_toxic_matches/total_matches*100),2)) + "% of matches were toxic!" )
    
    return total_matches
    
    
def get_total_toxic_players(match_summary: DataFrame,
                            total_matches:int
                            ):
    """
    Calculate the total players by multiplaying the amount of matches by 10, because there are 10
    players in a match.
    Get the amount of toxic players by doing a sum on the toxic player count. This was calculated by counting
    the distinct number of players who was toxic per game.
    Get the percentage of toxic players across all the games.
    """
    
    total_players = total_matches * 10
    total_toxic_players = match_summary.filter(F.col("toxic")==True).agg(F.sum("toxic_player_count")).collect()[0][0]
    print(str(round((total_toxic_players/total_players*100),2)) + "% of players were toxic!")
    
def get_most_toxic_region(match_summary: DataFrame,
                          total_matches: int):
    """
    Get a count of how many time each region appears in the toxic matches. We order this by the ttal amount in descending order,
    so that the most frequent region is at the top, and we take only the first result.
    """
    
    most_toxic_region = match_summary.filter(F.col("toxic")==True).groupBy("region").count().sort(F.col("count").desc()).limit(1)
    
    print(most_toxic_region.select("region").collect()[0][0] + " is the most toxic region with " + 
      str(round((most_toxic_region.select("count").collect()[0][0]/total_matches*100),2)) + "% of the toxic matches.")