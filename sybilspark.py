from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


spark = SparkSession \
        .builder \
        .appName("Python sybil finder") \
        .config("spark.memory.fraction", 0.8) \
        .config("spark.executor.memory", "24g") \
        .config("spark.driver.memory", "24g")\
        .config("spark.sql.shuffle.partitions" , "8000") \
        .getOrCreate()

sqlcontext = SQLContext(spark)
snapshot_file = 'data/2024-05-15-snapshot1_transactions.csv'
initial_file = 'data/initialList.csv'

snapshot_df = sqlcontext.read.load(snapshot_file,  \
                      format='com.databricks.spark.csv',  \
                      header='true',  \
                      inferSchema='true')


snapshot_df.registerTempTable("snapshot")

# initial list
filter_df = sqlcontext.read.load(initial_file,
                      format='com.databricks.spark.csv', \
                      header='true',  \
                      inferSchema='true')
filter_df.registerTempTable("filter")

summary_df = sqlcontext.sql("""SELECT SENDER_WALLET,  
                                count(*) as ntx,  
                                coalesce(SUM(NATIVE_DROP_USD),0) as amt,
                                coalesce(SUM(STARGATE_SWAP_USD),0) as samt,
                                count(distinct source_chain) as active_source_chain,
                                count(distinct destination_chain) as active_destinatin_chain,
                                min(SOURCE_TIMESTAMP_UTC) as ibt,
                                max(SOURCE_TIMESTAMP_UTC) as lbt,
                                project
                            from snapshot
                                WHERE 
                                Group by SENDER_WALLET, project
                                """)
summary_df.registerTempTable("summary")

sybil_df = sqlcontext.sql("""SELECT *
                            from summary
                                WHERE amt < 1
                                AND samt < 1
                                AND project <> ''
                                AND (active_destinatin_chain / active_source_chain) > 3
                                AND SENDER_WALLET NOT IN (SELECT address from filter) """)
#print(sybil_df.show())
import os
sybil_df.write.csv('data/sybil1')
os.system("cat data/sybil1/p* > data/sybil1.csv")