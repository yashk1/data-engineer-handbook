from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import broadcast
import os

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

    
def bucketed_matchdetails(spark):
    spark.sql("DROP TABLE IF EXISTS matchdetails_bucketed")
    matchdetails_df = spark.read.csv("../../../data/match_details.csv", header=True, inferSchema=True)

    matchdetails_df = matchdetails_df.repartition(16, "match_id").sortWithinPartitions("match_id")
    
    matchdetails_df.write.bucketBy(16, "match_id").format("parquet").mode("overwrite").saveAsTable("matchdetails_bucketed")
    matchdetails_bucketed = spark.table('matchdetails_bucketed')
    
    print('Match Details Bucketed')
    matchdetails_bucketed.show(5)
    
    return matchdetails_bucketed
    
def bucketed_matches(spark):
    spark.sql("DROP TABLE IF EXISTS matches_bucketed")
    matches_df = spark.read.csv("../../../data/matches.csv", header=True, inferSchema=True)
    matches_df = matches_df.repartition(16, "match_id").sortWithinPartitions("match_id")
    
    matches_df.write.bucketBy(16, "match_id").mode("overwrite").saveAsTable("matches_bucketed")
    matches_bucketed = spark.table('matches_bucketed')
    
    print('Matches Bucketed')
    matches_bucketed.show(5)
    
    return matches_bucketed
    
def bucketed_matches_v2(spark):
    num_buckets = 16

    # Create Matches DataFrame
    matches_df = spark.read.csv("../../../data/matches.csv", header=True, inferSchema=True)

    # Repartition and Sort Within Partitions
    spark.sql("DROP TABLE IF EXISTS matches_bucketed")
    matches_df = matches_df.repartition(num_buckets, "match_id").sortWithinPartitions("match_id", "mapid")
    matches_df.write.bucketBy(num_buckets, "match_id").mode("overwrite").saveAsTable("matches_bucketed_v2")

    # Read bucketed table
    matches_bucketed_df = spark.table("matches_bucketed_v2")
    return matches_bucketed_df


def bucketed_matches_v3(spark):
    num_buckets = 16

    # Create Matches DataFrame
    matches_df = spark.read.csv("../../../data/matches.csv", header=True, inferSchema=True)

    # Repartition and Sort Within Partitions
    spark.sql("DROP TABLE IF EXISTS matches_bucketed")
    matches_df = matches_df.repartition(num_buckets, "match_id").sortWithinPartitions("match_id", "playlist_id")
    matches_df.write.bucketBy(num_buckets, "match_id").mode("overwrite").saveAsTable("matches_bucketed_v3")

    # Read bucketed table
    matches_bucketed_df = spark.table("matches_bucketed_v3")
    return matches_bucketed_df

def compare_file_sizes():
    # Compare the file sizes of the three bucketed dataframes
    tables = ["matches_bucketed", "matches_bucketed_v2", "matches_bucketed_v3"]

    for table in tables:
        file_path = "spark-warehouse/" + table
        file_sizes = []
        if os.path.exists(file_path):
            for file in os.listdir(file_path):
                file_sizes.append(os.path.getsize(file_path + "/" + file))
        print(f"Table: {table}, File sizes: {file_sizes}")

    
def bucketed_medals_matches_players(spark):
    spark.sql("DROP TABLE IF EXISTS medals_matches_players_bucketed")
    medals_matches_players_df = spark.read.csv("../../../data/medals_matches_players.csv", header=True, inferSchema=True)
    medals_matches_players_df = medals_matches_players_df.repartition(16, "match_id").sortWithinPartitions("match_id")
    
    medals_matches_players_df.write.format("parquet").bucketBy(16, "match_id").mode("overwrite").saveAsTable("medals_matches_players_bucketed")
    medals_matches_players_bucketed = spark.table('medals_matches_players_bucketed')
    
    print('Medals Matches Players Bucketed')
    medals_matches_players_bucketed.show(5)
    
    return medals_matches_players_bucketed
    
def bucketjoin_matches_matchdetails_medalmatchesplayers(matches_bucketed, matchdetails_bucketed, medals_matches_players_bucketed):
    
    # Bucket join everything
    final_df = matches_bucketed.join(matchdetails_bucketed, on='match_id').join(medals_matches_players_bucketed, on=['match_id','player_gamertag'])
    return final_df

def get_stats(spark, final_df):

    # 1) Player with the highest average kills per game
    top_avg_kills = (
        final_df.groupBy("match_id", "player_gamertag")
                .agg(F.avg("player_total_kills").alias("avg_kills_per_game"))
                .orderBy(F.desc("avg_kills_per_game"))
                .limit(1)
    )

    # 2) Most played playlist
    most_played_playlist = (
        final_df.groupBy("playlist_id")
                .agg(F.countDistinct("match_id").alias("matches"))
                .orderBy(F.desc("matches"))
                .limit(1)
    )

    # 3) Most played map
    most_played_map = (
        final_df.groupBy("mapid")
                .agg(F.countDistinct("match_id").alias("matches"))
                .orderBy(F.desc("matches"))
                .limit(1)
    )
    
    # Join with medals DataFrame
    medals_df = spark.read.csv("../../../data/medals.csv", header=True, inferSchema=True)

    final_df_joined = final_df.join(broadcast(medals_df), "medal_id")
    
    # 4) Map with the most Killing Spree medals
    killing_sprees = (
        final_df_joined.filter(F.col("classification") == "KillingSpree")
                      .groupBy("mapid")
                      .count()
                      .orderBy(F.desc("count"))
                      .limit(1)
    )

    # Display results
    print("Top Player by Average Kills per Game:")
    top_avg_kills.show()

    print("\nMost Played Playlist:")
    most_played_playlist.show()

    print("\nMost Played Map:")
    most_played_map.show()

    print("\nMap with the Most Killing Spree Medals:")
    killing_sprees.show()


def get_df_heads(spark):
    matches_df = spark.read.csv("../../../data/matches.csv", header=True, inferSchema=True)
    maps_df = spark.read.csv("../../../data/maps.csv", header=True, inferSchema=True)
    medals_matches_players_df = spark.read.csv("../../../data/medals_matches_players.csv", header=True, inferSchema=True)
    medals_df = spark.read.csv("../../../data/medals.csv", header=True, inferSchema=True)

    print("Matches DataFrame:")
    matches_df.show(5)
    print(f"Total rows in Matches DataFrame: {matches_df.count()}")

    print("Maps DataFrame:")
    maps_df.show(5)
    print(f"Total rows in Maps DataFrame: {maps_df.count()}")

    print("Medals Matches Players DataFrame:")
    medals_matches_players_df.show(5)
    print(f"Total rows in Medals Matches Players DataFrame: {medals_matches_players_df.count()}")

    print("Medals DataFrame:")
    medals_df.show(5)
    print(f"Total rows in Medals DataFrame: {medals_df.count()}")

def main():
    spark = SparkSession.builder \
        .config("spark.sql.autoBroadcastJoinThreshold", -1)\
        .appName("MatchStats") \
        .getOrCreate()
        
    # Retrieve memory settings
    sc = spark.sparkContext

    driver_memory = sc.getConf().get("spark.driver.memory", "Default not set")
    executor_memory = sc.getConf().get("spark.executor.memory", "Default not set")

    print(f"Driver memory: {driver_memory}")
    print(f"Executor memory: {executor_memory}")
    
    # Get heads of all DataFrames
    # get_df_heads(spark)
    
    # Perform bucketed operations
    bucketed_matchdetails_df = bucketed_matchdetails(spark)
    bucketed_matches_df = bucketed_matches(spark)
    bucketed_medals_matches_players_df = bucketed_medals_matches_players(spark)
    
    # Perform bucket join
    final_df = bucketjoin_matches_matchdetails_medalmatchesplayers(bucketed_matches_df, bucketed_matchdetails_df, bucketed_medals_matches_players_df)
    
    # Show final result
    final_df.show(5)
        
    # get stats
    get_stats(spark, final_df)
    
    bucketed_matches_v2 = bucketed_matches_v2(spark)
    bucketed_matches_v3 = bucketed_matches_v3(spark)
    
    compare_file_sizes()
    
if __name__ == "__main__":
    main()