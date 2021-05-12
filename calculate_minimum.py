from pyspark.sql import SparkSession
import pyspark.sql.functions as fun

#after importing the spark session, lets create our spark program
spark = SparkSession.builder.appName("Spark Assignment question1").getOrCreate()

#import the data set and set header to be true to properly format the column names
df = spark.read.format("csv").option("inferSchema", "true").load('/user/common_data/Spark_Assignment_Dataset.csv',header =True, inferSchema=True)

#showing first 10 elements in the table
df.show(10)

#Here we want to show the column with the minimum Pressure3pm from Launceston location
print ("==========================we are printing minimum value at Launceston============================")
minPressure = df.filter(df.Location=="Launceston").agg({ "Pressure3pm":"min"} ).collect()

print("\n\n ===================The Minimum pressure of Pressure3pm at Launceston is ================================")
print(minPressure)
print("=============================================================================================================")
print("\n\n")

#===============================================================================================================================================
#=====================Alternatively, can use this to print and save the data to a csv file=====================================================
#minPressure = df.filter(df.Location=="Launceston").select(fun.min("Pressure3pm")).alias("Minimum Pressure in Launceston")

#showing the minimum value
#minPressure.show(5)

#minPressure.groupBy().agg(min( "Pressure3pm"))


#saving it to csv at the output path named sparkOutput
#minPressure.write.format("csv").save("sparkOutput_test")
