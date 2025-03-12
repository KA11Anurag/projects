import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format
from pyspark.sql.functions import to_date, count, col
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
    

    # Solution 1
    # rideshare_data.csv is loaded from S3 bucket //data-repository-bkt/ECS765/rideshare_2023/ into Dataframe rideshare_data using spark read csv API
    # taxi_zone_lookup.csv is loaded from S3 bucket //data-repository-bkt/ECS765/rideshare_2023/ into Dataframe taxi_zone_lookup_df using spark read csv API
    rideshare_data = spark.read.option("delimiter", ",").option("header", True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv")
    taxi_zone_lookup_df = spark.read.option("delimiter",",").option("header", True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv")

    # Solution 2
    # Stage 1
    # used join API to combine rideshare_data using column pickup_location and taxi_zone_lookup_df using column LocationID and stored the result in joined_initial_data DF
    # deleted taxi_zone_lookup_df column LocationID after join to avoid redundancy using drop API; renamed columns using withColumnRenamed API
    joined_initial_data = rideshare_data.join(taxi_zone_lookup_df, [rideshare_data["pickup_location"] == taxi_zone_lookup_df["LocationID"]]) \
                                        .drop(taxi_zone_lookup_df["LocationID"]) \
                                        .withColumnRenamed("Borough", "Pickup_Borough") \
                                        .withColumnRenamed("Zone", "Pickup_Zone") \
                                        .withColumnRenamed("service_zone", "Pickup_service_zone")
    
    # Stage 2
    # used join API to combine joined_initial_data using column dropoff_location and taxi_zone_lookup_df using column LocationID and stored the result in joined_data DF
    # deleted taxi_zone_lookup_df column LocationID after join to avoid redundancy using drop API; renamed columns using withColumnRenamed API
    joined_data = joined_initial_data.join(taxi_zone_lookup_df, [joined_initial_data["dropoff_location"] == taxi_zone_lookup_df["LocationID"]])\
                                     .drop(taxi_zone_lookup_df["LocationID"]) \
                                     .withColumnRenamed("Borough", "Dropoff_Borough") \
                                     .withColumnRenamed("Zone", "Dropoff_Zone") \
                                     .withColumnRenamed("service_zone", "Dropoff_service_zone")
    # displayed schema structure
    joined_data.printSchema()

    # Solution 3
    # used withColumn API to update the date column from UNIX timestamp to format 'yyyy-MM-dd' using from_unixtime module in pyspark.sql.functions

    # display initial schema before timestamp conversion
    print(joined_data.show(10))

    # performed update
    joined_data = joined_data.withColumn('date', from_unixtime(joined_data['date'], "yyyy-MM-dd"))
    
    # display schema after timestamp conversion
    print(joined_data.show(10))
    
    # Solution 4
    # used count API to get the total number of rows in the DF and used show to display the first 10 rows of the DF in tabular format
    print("The total Row Count of the Dataset is {}".format(joined_data.count()))

    print(joined_data.show(10))

    # code to get the current timestamp and convert it in string format
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    # optional code to store result back on S3 bucket
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    # used write.csv attribute to write the joined dataset to my S3 bucket
    joined_data.write.csv("s3a://" + s3_bucket + '/mergedData_' + date_time, header=True, mode="overwrite")

    # closed the current SparkSession
    spark.stop()
