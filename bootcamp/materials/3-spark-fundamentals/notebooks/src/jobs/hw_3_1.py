from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast



def main():
    spark = SparkSession.builder \
        .config('spark.sql.autoBroadcastJoinThreshold', -1) \
        .appName('hw_3_1').getOrCreate()
        
    
if __name__ == "__main__":
    main()