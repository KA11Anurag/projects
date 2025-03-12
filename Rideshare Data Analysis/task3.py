import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import row_number, count, col, month, sum, desc, concat, lit
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType
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

    # used withColumn API to create month column containing month extracted using 'month' API from the date column
    # updated dataset is stored in new df dataframe
    df = merged_data.withColumn("month", month(merged_data["date"]))

    

    # Solution 1
    # defined a window function by partitionBy API to divide the dataset in blocks of months, ordered in descending value of trip count using 'orderBy' and 'desc' function
    partition = Window.partitionBy("month").orderBy(desc("trip_count"))

    # used groupBy API to group the dataset on Pickup_Borough and month columns, used agg function 'count' to get the trip counts
    # used withColumnRenamed to rename the obtained 'count' column to 'trip_count'
    # used rank API 'row_number()' to generate sequential numbers in 'id' column w.r.t 'partition' defined earlier, highest trip_count each month gets number 1
    # used filter API to extract the top 5 pickup boroughs for each month
    # deleted 'id' column using drop API
    # arranged the final result using orderBy API on increasing and decreasing value of month and trip_count columns
    # stored the modified dataframe in top_popular_pickup
    top_popular_pickup = df.groupBy("Pickup_Borough","month").count() \
                           .withColumnRenamed("count", "trip_count") \
                           .withColumn("id", row_number().over(partition)) \
                           .filter(col("id")<=5) \
                           .drop("id") \
                           .orderBy(["month", "trip_count"], ascending =[True, False])

    # displayed top 5 pickup boroughs for each month in tabular format using show function
    print(top_popular_pickup.show(25))

    
    
    # Solution 2
    # defined a window function by partitionBy API to divide the dataset in blocks of months, ordered in descending value of trip count using 'orderBy' and 'desc' function
    partition = Window.partitionBy("month").orderBy(desc("trip_count"))

    # used groupBy API to group the dataset on Dropoff_Borough and month columns, used agg function 'count' to get the trip counts
    # used withColumnRenamed to rename the obtained 'count' column to 'trip_count'
    # used rank API 'row_number()' to generate sequential numbers in 'id' column w.r.t 'partition' defined earlier, highest trip_count each month gets number 1
    # used filter API to extract the top 5 dropoff boroughs for each month
    # deleted 'id' column using drop API
    # arranged the final result using orderBy API on increasing and decreasing value of month and trip_count columns
    # stored the modified DF in top_popular_dropoff
    top_popular_dropoff = df.groupBy("Dropoff_Borough","month").count() \
                            .withColumnRenamed("count", "trip_count") \
                            .withColumn("id", row_number().over(partition)) \
                            .filter(col("id")<=5) \
                            .drop("id") \
                            .orderBy(["month", "trip_count"], ascending =[True, False])

    # displayed top 5 dropoff boroughs for each month in tabular format using show function
    print(top_popular_dropoff.show(25))

    
    
    # Solution 3
    # used withColumn API to create a new column 'Route' by combining Pickup_Borough and Dropoff_Borough columns seperated by 'to', using concat API
    # used withColumn API to update the datatype of driver_total_pay column from string to float using cast function
    # used groupBy API to group the dataset on 'Route' column, used agg function 'sum' on driver_total_pay column values
    # used withColumnRenamed API to rename the calculated 'sum(driver_total_pay)' column to 'total_profit'
    # used withColumn API on 'total_profit' column to change the numerical representation from scientific to decimal notation
    # used orderBy function to arrange the dataset w.r.t decreasing 'total_profit' values, stored the modified DF in top_earnest_routes
    top_earnest_routes = df.withColumn("Route", concat(col("Pickup_Borough"), lit(" to "), col("Dropoff_Borough"))) \
                           .withColumn("driver_total_pay", col("driver_total_pay").cast('float')) \
                           .groupBy("Route").sum("driver_total_pay") \
                           .withColumnRenamed("sum(driver_total_pay)", "total_profit") \
                           .withColumn("total_profit", col("total_profit").cast(DecimalType(precision=12, scale=3))) \
                           .orderBy("total_profit", ascending = False)

    # displayed top 30 earnest routes using show function, used 'truncate = False' to avoid shortening large strings of each rows
    print(top_earnest_routes.show(30, truncate=False))


    # code to get the current timestamp and convert it in string format
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    # optional code to store result back on S3 bucket
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)


    # optional, write.csv to store the schemas top_popular_pickup, top_popular_dropoff, top_earnest_routes in a csv file
    # top_popular_pickup.write.csv("s3a://" + s3_bucket + '/top_popular_pickup_' + date_time, header=True, mode="overwrite")
    # top_popular_dropoff.write.csv("s3a://" + s3_bucket + '/top_popular_dropoff_' + date_time, header=True, mode="overwrite")
    # top_earnest_routes.write.csv("s3a://" + s3_bucket + '/top_earnest_routes_' + date_time, header=True, mode="overwrite")


    # closed the current SparkSession
    spark.stop()
