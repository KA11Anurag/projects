import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import count, col, concat, lit
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

    # used withColumn API to create a new column 'Route' by combining Pickup_Zone and Dropoff_Zone columns seperated by 'to',using concat API
    # used groupBy API to group dataset on 'Route' and 'business' columns, used agg function 'count' to get the trip counts
    # used withColumnRenamed to rename the obtained 'count' column to 'uber_count'
    # used filter API to extract the trip counts for different unique 'Pickup_Zone' and 'Dropoff_Zone' for Uber
    # deleted 'business' column using drop API, stored the modified DF in uber_route_count
    uber_route_count = merged_data.withColumn("Route", concat(col("Pickup_Zone"), lit(" to "), col("Dropoff_Zone"))) \
                                  .groupBy("Route", "business").count() \
                                  .withColumnRenamed("count", "uber_count") \
                                  .filter(col("business") == "Uber") \
                                  .drop(col("business"))

    # used withColumn API to create a new column 'Route' by combining Pickup_Zone and Dropoff_Zone columns seperated by 'to',using concat API
    # used groupBy to group the dataset on 'Route' and 'business'columns, used agg function 'count' to get the trip counts
    # used withColumnRenamed to rename the obtained 'count' column to 'lyft_count'
    # used filter API to extract the trip counts for different unique 'Pickup_Zone' and 'Dropoff_Zone' for Lyft
    # deleted 'business' column using drop API, stored the modified DF in lyft_route_count

    lyft_route_count = merged_data.withColumn("Route", concat(col("Pickup_Zone"), lit(" to "), col("Dropoff_Zone"))) \
                                  .groupBy("Route", "business").count() \
                                  .withColumnRenamed("count", "lyft_count") \
                                  .filter(col("business") == "Lyft") \
                                  .drop(col("business"))

    ## Solution 1
    # used join API to combine 'uber_route_count' using column 'Route' and lyft_route_count using column 'Route'
    # deleted 'Route' column of lyft_route_count after join to avoid redundancy
    # used withColumn API to get column 'total_count' as the sum of Uber and Lyft counts on the same route
    # used orderBy API to sort the data in decreasing values of total_count column, stored the modified DF in result_schema
    result_schema = uber_route_count.join(lyft_route_count, (uber_route_count["Route"] == lyft_route_count["Route"])) \
                                    .drop(lyft_route_count["Route"]) \
                                    .withColumn("total_count", (col("uber_count") + col("lyft_count"))) \
                                    .orderBy("total_count", ascending = False)

    # displayed top 10 popular routes in tabular format using show function, used 'truncate = False' to avoid shortening large strings
    print(result_schema.show(10, truncate = False))

    # closed the current SparkSession
    spark.stop()
