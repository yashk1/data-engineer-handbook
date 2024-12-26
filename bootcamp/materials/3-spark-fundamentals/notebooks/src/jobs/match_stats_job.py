from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

def broadcastjoin_matches_and_maps(spark):
    matches_df = spark.read.csv("../../../data/matches.csv", header=True, inferSchema=True)
    maps_df = spark.read.csv("../../../data/maps.csv", header=True, inferSchema=True)
    
    matches_maps_df = matches_df.join(broadcast(maps_df), on='mapid')
    
    return matches_maps_df
    
def broadcastjoin_medalsmatchesplayers_medals(spark):
    medals_matches_players_df = spark.read.csv("../../../data/medals_matches_players.csv", header=True, inferSchema=True) 
    medals_df = spark.read.csv("../../../data/medals.csv", header=True, inferSchema=True)

    medals_players_df = medals_matches_players_df.join(broadcast(medals_df), on='medal_id')

    return medals_players_df

def bucketjoin_matches_matchdetails_medalmatchesplayers(spark):
    matches_df = spark.read.csv("../../../data/matches.csv", header=True, inferSchema=True)
    match_details = spark.read.csv("../../../data/match_details.csv", header=True, inferSchema=True)
    medals_matches_players_df = spark.read.csv("../../../data/medals_matches_players.csv", header=True, inferSchema=True) 
    
    
def bucketed_match_details(spark):
    matches_df = spark.read.csv("../../../data/medals_matches_players.csv", header=True, inferSchema=True)

    matches_df = matches_df.repartition(16, "match_id").sortWithinPartitions("match_id")
    
    matches_df.write.bucketBy(16, "match_id").saveAsTable("matches_bucketed")
    spark.sql("SELECT * FROM matches_bucketed").show(4)
    
    
def main():
    spark = SparkSession.builder\
        .config("spark.sql.broadcastThreshold", -1)\
        .appName("MatchStats")\
        .getOrCreate()
        
    broadcast_maps = broadcastjoin_matches_and_maps(spark)
    broadcast_medals = broadcastjoin_medalsmatchesplayers_medals(spark)
    # bucketjoin_matches_matchdetails_medalmatchesplayers(spark)
    bucketed_match_details(spark)
        
if __name__ == "__main__":
    main()