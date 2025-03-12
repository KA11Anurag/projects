import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format, concat
from pyspark.sql.functions import to_date, count, col, month, sum, lit
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

    # read the stored joined_data DF created in task 1 for executing Task 2 problems
    joined_data = spark.read.option("delimiter", ",").option("header", True).csv("s3a://" + s3_bucket + "/mergedData_07-03-2024_22:16:20/")
    
     
    # Solution 1
    # used withColumn API to create month column containing month extracted from the date column using pyspark.sql.functions,month
    # used groupBy API to group the dataset on the basis of bussiness and month columns, used agg function 'count' to get the trip counts
    # used withColumn API to create a new column 'business-month' by combining business and month column seperated by '-', using concat API
    # used orderBy API to sort the DF based on ascending order of month, stored the modified DF in business_trip
    # used select to query the business_trip schema to select columns business-month and trip count,renamed as counts using alias function
    business_trip = joined_data.withColumn("month", month(joined_data["date"])) \
                               .groupBy("business", "month").count() \
                               .withColumn("business-month", concat(col("business"), lit("-"), col("month"))) \
                               .orderBy("month", ascending = True)
    business_trip = business_trip.select("business-month", col("count").alias("counts"))

    # Solution 2
    # used withColumn API to create month column containing month extracted from the date column using pyspark.sql.functions,month
    # used withColumn API to update the datatype of rideshare_profit column from string to float using cast function
    # used groupBy API to group the dataset on bussiness and month columns, used agg function 'sum' on rideshare_profit column values
    # used withColumn API to create a new column 'business-month' by combining business and month column seperated by '-', using concat API
    # stored the modified DF in new platforms_profit DF
    platforms_profit = joined_data.withColumn("month", month(joined_data["date"])) \
                                  .withColumn("rideshare_profit", joined_data["rideshare_profit"].cast('float'))\
                                  .groupBy("business", "month").sum("rideshare_profit") \
                                  .withColumn("business-month", concat(col("business"), lit("-"), col("month")))
                                  
                                  
    # used select to query platforms_profit columns business-month and rideshare_profit sum renamed as platform's profits using alias
    # updated the dataframe to represent the selected columns
    platforms_profit = platforms_profit.select("business-month", col("sum(rideshare_profit)").alias("platform's profits"))
    

    # Solution 3
    # used withColumn API to create month column containing month extracted from the date column using pyspark.sql.functions,month
    # used withColumn API to update the datatype of driver_total_pay column from string to float using cast function
    # used groupBy API to group the dataset on bussiness and month column , used agg function 'sum' on driver_total_pay column values
    # used withColumn API to create a new column 'business-month' by combining business and month column seperated by '-',using concat API
    # stored the modified DF in new drivers_earnings DF
    drivers_earnings = joined_data.withColumn("month", month(joined_data["date"])) \
                                  .withColumn("driver_total_pay", joined_data["driver_total_pay"].cast('float'))\
                                  .groupBy("business", "month").sum("driver_total_pay") \
                                  .withColumn("business-month", concat(col("business"), lit("-"), col("month")))

    # used select to query drivers_earnings columns business-month and driver_total_pay sum renamed as driver's earnings using alias
    # updated the dataframe to represent the selected columns
    drivers_earnings = drivers_earnings.select("business-month", col("sum(driver_total_pay)").alias("driver's earnings"))

    # code to get the current timestamp and convert it in string format
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    # optional code to store result back on S3 bucket
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    # used write.csv and coalesce attributes to store business_trip, platforms_profit, drivers_earnings each in single csv file format,in S3 bucket
    business_trip.coalesce(1).write.csv("s3a://" + s3_bucket + '/business_trip_' + date_time, header=True, mode="overwrite")
    platforms_profit.coalesce(1).write.csv("s3a://" + s3_bucket + '/platforms_profit_' + date_time, header=True, mode="overwrite")
    drivers_earnings.coalesce(1).write.csv("s3a://" + s3_bucket + '/drivers_earnings_' + date_time, header=True, mode="overwrite")
    
    # closed the current SparkSession
    spark.stop()
