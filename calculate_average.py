from pyspark.sql import SparkSession
import pyspark.sql.functions as fun

#after importing the spark session, lets create our spark program
spark = SparkSession.builder.appName("Spark Assignment question2").getOrCreate()

#import the data set and set header to be true to properly format the column names
df = spark.read.format("csv").option("inferSchema", "true").load('/user/common_data/Spark_Assignment_Dataset.csv',header =True, inferSchema=True)

#showing first 10 elements in the table
df.show(10)

#Here we want to show the column with the average Pressure3pm from Launceston location
print ("==========================we are printing average value at Launceston============================")
avgPressure = df.filter(df.Location=="Launceston").agg({ "Pressure3pm":"avg"} ).collect()

print("\n\n ===================The Average pressure of Pressure3pm at Launceston is ================================")
print(avgPressure)
print("=============================================================================================================")
print("\n\n")


'''
#=====================Alternatively, can use this to print and save the data to a csv file=====================================================
avgPressure = df.filter(df.Location=="Launceston").select(fun.avg("Pressure3pm")).alias("Minimum Temperature in Launceston")

#showing the minimum value
avgPressure.show()

#saving it to csv at the output path named sparkOutput
avgPressure.write.format("csv").save("sparkOutput2")
'''
