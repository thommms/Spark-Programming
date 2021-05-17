from __future__ import print_function
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Spark Assignment question5").getOrCreate()

print("==========================I will generate the schema for the tweet data here=======================================")
twitterSchema = spark\
                .read.json('/user/tokonkw/json_tweet_data.json',multiLine = "true")\
                .schema

print("===========================I will pass the schema here==============================================")
df=spark.readStream.format("json")\
        .schema(twitterSchema)\
        .option("maxFilesPerTrigger",1)\
        .load('/user/tokonkw/json_tweet_data.json')

print("==================================I am starting the streaming here=========================================")
dfStream = df.writeStream\
                .queryName("Parsing_Query")\
                .trigger(processingTime='10 seconds')\
                .format("json")\
                .outputMode("append")\
                .option("path", '/user/tokonkw/trigger_output')\
                .option("checkpointLocation", "/user/tokonkw/checkpointTweet")

print("================================Streaming started===========================================")
dfStream.start()
