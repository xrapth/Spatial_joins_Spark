# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import split
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, collect_list, struct
from pyspark.sql.functions import *
from pyspark.sql import Window
from haversine import haversine
import matplotlib.pyplot as plt
from pyspark.sql.functions  import spark_partition_id
import time
start_time = time.time()

hotels = spark.sparkContext.textFile("/FileStore/tables/hotels_ver1-ea294.txt")

restaurants = spark.sparkContext.textFile("/FileStore/tables/restaurants_ver1-4694f.txt")

hot = hotels.map(lambda x: (x.split('|')[0], x.split('|')[1], x.split('|')[4], x.split('|')[5]))
hot.take(5)

res = restaurants.map(lambda x: (x.split('|')[0], x.split('|')[1], x.split('|')[3], x.split('|')[4]))
res.take(5)

#res100 = res.filter(lambda x: int(x[0])<10001)

#hot100 = hot.filter(lambda x: int(x[0])<10001)

resDF = spark.createDataFrame(res).cache()
resDF.show()

hotDF = spark.createDataFrame(hot).cache()
hotDF.show()

hotDF.schema
hotDF = hotDF.withColumn("_3int", hotDF["_3"].cast('integer'))
hotDF = hotDF.withColumn("_4int", hotDF["_4"].cast('integer'))
hotDF = hotDF.withColumn("_3", hotDF["_3"].cast('float'))
hotDF = hotDF.withColumn("_4", hotDF["_4"].cast('float'))
hotDF.show(5) 
hotDF.schema

resDF.schema
resDF = resDF.withColumn("_3int", resDF["_3"].cast('integer'))
resDF = resDF.withColumn("_4int", resDF["_4"].cast('integer'))
resDF = resDF.withColumn("_3", resDF["_3"].cast('float'))
resDF = resDF.withColumn("_4", resDF["_4"].cast('float'))
resDF.show(5)
resDF.schema

hotDF_lat = hotDF.orderBy("_3int", ascending= True)

resDF_lat = resDF.orderBy("_3int", ascending= True)

resPART = resDF_lat.repartitionByRange(4, col("_3int"))
resPART.show()

print(resPART.rdd.getNumPartitions()) 

resPART.withColumn("partitionId", spark_partition_id()).groupBy("partitionId").count().show()

hotPART = hotDF_lat.repartitionByRange(4, col("_3int"))
hotPART.show()

print(hotPART.rdd.getNumPartitions()) 

hotPART.withColumn("partitionId", spark_partition_id()).groupBy("partitionId").count().show()

hotelsNEW = hotPART.select('_1','_2','_3','_4','_3int','_4int').withColumnRenamed("_1","Hotel_Id").withColumnRenamed("_2","Hotel_Name").withColumnRenamed("_3","Hotel_Lat").withColumnRenamed("_4","Hotel_Lon").withColumnRenamed("_3int","Hotel_Lat_int").withColumnRenamed("_4int","Hotel_Lon_int")
hotelsNEW.show()

restaurantsNEW = resPART.select('_1','_2','_3','_4','_3int','_4int').withColumnRenamed("_1","Restaurant_Id").withColumnRenamed("_2","Restaurant_Name").withColumnRenamed("_3","Restaurant_Lat").withColumnRenamed("_4","Restaurant_Lon").withColumnRenamed("_3int","Restaurant_Lat_int").withColumnRenamed("_4int","Restaurant_Lon_int")
restaurantsNEW.show()

DF_join = restaurantsNEW.join(hotelsNEW ,restaurantsNEW['Restaurant_Lat_int'] == hotelsNEW['Hotel_Lat_int'] ,how = 'cross')
DF_join.show()

DF_join.na.drop()
DF_join.show()
DF_join.count()

d = DF_join.rdd.map(lambda x:(x[0], x[1], x[6], x[7], haversine(( x[2] , x[3] ),( x[8] , x[9] )) ))

distance = d.toDF()
#distance.show()

Distance_HOT_RES = distance.filter(distance[4]< 0.5)

Distance_HOT_RES.show()

endTimeQuery = time.clock()

end_time = time.time()

print("Total execution time: {} seconds".format(end_time - start_time))

time = [12, 13, 14, 21]
size = [100, 1000, 10000, 'ALL']

plt.plot(size, time)
plt.xlabel('Data size')
plt.ylabel('Execution time (sec)')

time1 = [0, 25.3, 24.7, 24.9, 24.6, 25.1]
theta = [0, 0.2, 0.4, 0.6, 0.8, 1.0]

plt.plot(theta, time1)
plt.xlabel('θ')
plt.ylabel('Execution time (sec)')




