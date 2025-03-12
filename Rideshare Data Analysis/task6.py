import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, sum
from graphframes import *



if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("TestDataset")\
        .getOrCreate()
    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")


    # loaded the stored csv of joined_data DF,created in task 1,from S3 bucket to 'merged_data' using read API
    merged_data = spark.read.option("delimiter", ",").option("header", True).csv("s3a://" + s3_bucket + "/mergedData_07-03-2024_22:16:20/")
    

    
    
    # Solution 1
    # used groupBy API to group the dataset on Pickup_Borough and time_of_day columns, used agg function 'count' to get the trip counts
    # used withColumnRenamed to rename the obtained 'count' column to 'trip_count'
    # used filter API to extract trip counts greater than 0 and less than 1000 for different 'Pickup_Borough' at different 'time_of_day'
    # used orderBy API to sort the data on decreasing trip_count column values, stored the modified DF in borough_trip_count
    borough_trip_count = merged_data.groupBy("Pickup_Borough", "time_of_day").count() \
                                    .withColumnRenamed("count", "trip_count") \
                                    .filter((col("trip_count") > 0) & (col("trip_count") < 1000)) \
                                    .orderBy("trip_count", ascending = False)

    # displayed trip counts in tabular format using show function
    print(borough_trip_count.show())
    


    
    # Solution 2
    # used groupBy API to group the dataset on Pickup_Borough and time_of_day columns, used agg function 'count' to get the trip counts
    # used withColumnRenamed to rename the obtained 'count' column to 'trip_count'
    # used filter API to extract the evening time_of_day for different 'Pickup_Borough'
    # used orderBy API to sort the data on descending trip_count column values, stored the modified DF in evening_trip_count
    evening_trip_count = merged_data.groupBy("Pickup_Borough", "time_of_day").count() \
                                    .withColumnRenamed("count", "trip_count") \
                                    .filter(col("time_of_day") == "evening") \
                                    .orderBy("trip_count", ascending = False)

    # displayed trip counts in tabular format using show function
    print(evening_trip_count.show())
    


    # Solution 3
    # used groupBy API to group the dataset on Pickup_Borough, Dropoff_Borough and Pickup_Zone columns, used agg function 'count' to get the trip counts
    # used withColumnRenamed to rename the obtained 'count' column to 'trip_count'
    # used filter API to extract the number of trips that started in Brooklyn and ended in Staten Island
    # used orderBy API to sort the data on descending trip_count column values, stored the modified DF in 'required_trip_'
    required_trip_ = merged_data.groupBy("Pickup_Borough", "Dropoff_Borough", "Pickup_Zone").count() \
                               .withColumnRenamed("count", "trip_count") \
                               .filter((col("Pickup_Borough") == "Brooklyn") & (col("Dropoff_Borough") == "Staten Island")) \
                               .orderBy("trip_count", ascending = False)

    # used select API to query the required_trip_ columns Pickup_Borough,Dropoff_Borough and Pickup_Zone
    # stored the selected columns in a new DF required_trip
    # displayed top 10 number of trips in tabular format using show function,used 'truncate = False' to avoid shortening large strings
    required_trip = required_trip_.select("Pickup_Borough", "Dropoff_Borough", "Pickup_Zone")
    print(required_trip.show(10, truncate = False))

    # used aggregate function 'sum' to calculate total number of trips from Brooklyn to Staten Island
    # displayed the sum in tabular format using the show function
    required_trip_count = required_trip_.agg(sum("trip_count").alias("total number of trips"))
    print(required_trip_count.show())

    # closed the current SparkSession
    spark.stop()
