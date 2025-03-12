import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import row_number, count, col, month, sum, desc, concat
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
    # used withColumn API to update the datatype of driver_total_pay column from string to float using cast function
    # used groupBy API to group the dataset on time_of_day column, used agg function 'avg' on driver_total_pay column values
    # used withColumnRenamed API to rename the obtained 'avg(driver_total_pay)' column to 'average_drive_total_pay'
    # used orderBy API to sort data in decreasing average driver's pay value, stored the modified DF in driver_total_pay_avg
    driver_total_pay_avg = merged_data.withColumn("driver_total_pay", col("driver_total_pay").cast('float')) \
                                 .groupBy("time_of_day").avg("driver_total_pay") \
                                 .withColumnRenamed("avg(driver_total_pay)", "average_drive_total_pay") \
                                 .orderBy("average_drive_total_pay", ascending = False)

    # displayed highest average driver's pay during different time of day in tabular format using show function
    print(driver_total_pay_avg.show())

    

    # Solution 2
    # used withColumn API to update the datatype of trip_length column from string to float using cast function
    # used groupBy API to group the dataset on time_of_day column, used agg function 'avg' on trip_length column values
    # used withColumnRenamed API to rename the obtained 'avg(trip_length)' column to 'average_trip_length'
    # used orderBy API to sort data in decreasing average trip length value, stored the modified DF in trip_length_avg
    trip_length_avg = merged_data.withColumn("trip_length", col("trip_length").cast('float')) \
                                 .groupBy("time_of_day").avg("trip_length") \
                                 .withColumnRenamed("avg(trip_length)", "average_trip_length") \
                                 .orderBy("average_trip_length", ascending = False)

    # displayed highest average trip length during different time of day in tabular format using show function
    print(trip_length_avg.show())


    

    # Solution 3
    # used join API to combine driver_total_pay_avg using column time_of_day and trip_length_avg using column time_of_day and stored the result in average_earned_per_mile
    # deleted trip_length_avg column 'trip_length_avg' after join to avoid redundancy using drop API
    # used withColumn API to create column 'average_earning_per_mile', containing data obtained by dividing column values of average_drive_total_pay, average_trip_length
    # used orderBy API to sort the DF in decreasing order of average_earning_per_mile column
    # used select function to query the average_earned_per_mile schema to select columns time_of_day and average_earning_per_mile
    # updated the average_earned_per_mile schema to have the required columns
    average_earned_per_mile = driver_total_pay_avg.join(trip_length_avg, [driver_total_pay_avg["time_of_day"] == trip_length_avg["time_of_day"]]) \
                                                  .drop(trip_length_avg["time_of_day"]) \
                                                  .withColumn("average_earning_per_mile", (col("average_drive_total_pay") / col("average_trip_length"))) \
                                                  .orderBy("average_earning_per_mile", ascending = False)
    average_earned_per_mile = average_earned_per_mile.select("time_of_day", "average_earning_per_mile")

    # displayed average earned per mile during different time of day in tabular format using show function
    print(average_earned_per_mile.show())
    

    # closed the current SparkSession
    spark.stop()
