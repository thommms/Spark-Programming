from pyspark.sql import SparkSession

#after importing the spark session, lets create our spark program
spark = SparkSession.builder.appName("Spark Assignment question3").getOrCreate()

#import the data set  and set header to be true to  properly format the column names
df = spark.read.csv('/user/common_data/Spark_Assignment_Dataset.csv',header =True, inferSchema=True)

#lets show what is inside the dataframe which we imported the data on. we will show first 10
df.show(10)

#created a name to represent the dataframe
df.createOrReplaceTempView("sparkDataset")

#Here we want to show number of pressure3pm count based on the Launceston location
print ("=================printing the count value of pressure3pm at Launceston==============================")
sqlDF = spark.sql("SELECT count(Pressure3pm) from sparkDataset where Location = 'Launceston'")

print("====================================================The total count for Pressure3pm at Launceston is ================================================")
sqlDF.show()
print("=====================================================================================================================================================")
print("\n\n")

print("==========please check the output path /home/tokonkw/sparkOutputCount for the csv file containing the output shown above=================================")
sqlDF.write.format("csv").save("sparkOutputCount")
print ("\n")
