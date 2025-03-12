import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, month, avg, dayofmonth
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
    # used withColumn API to create month column containing month extracted from the date column using pyspark.sql.functions 'month'
    # used withColumn API to create day column containing day extracted from the date column using pyspark.sql.functions 'dayofmonth'
    # used withColumn API to update the datatype of request_to_pickup column from 'string' to 'int' using cast function
    # used filter API to extract data for all the days of month January
    # used groupBy API to group the dataset on day column, used agg function 'avg' on request_to_pickup column values
    # used withColumnRenamed API to rename the obtained 'avg(request_to_pickup)' column to 'average waiting time'
    # used orderBy API to sort the data based on 'day' column in increasing order, stored the modified DF in average_waiting_time
    average_waiting_time = merged_data.withColumn("month", month(merged_data["date"])) \
                                      .withColumn("day", dayofmonth(merged_data["date"])) \
                                      .withColumn("request_to_pickup", col("request_to_pickup").cast('int')) \
                                      .filter(col("month") == 1) \
                                      .groupBy("day").avg("request_to_pickup") \
                                      .withColumnRenamed("avg(request_to_pickup)", "average waiting time") \
                                      .orderBy("day", ascending = True)

    # displayed average waiting time in January in tabular format using show function
    print(average_waiting_time.show(35))

    # code to get the current timestamp and convert it in string format
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    # optional code to store result back on S3 bucket
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    # used write.csv and coalesce attributes to store the schemas average_waiting_time in a single csv file format, in S3 bucket
    average_waiting_time.coalesce(1).write.csv("s3a://" + s3_bucket + '/average_waiting_time_' + date_time, header=True, mode="overwrite")

    # closed the current SparkSession
    spark.stop()
