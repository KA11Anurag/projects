import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

from functools import reduce
from pyspark.sql.functions import col, lit, when
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
import graphframes
from graphframes import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.2-s_2.12") \
        .appName("graphframes") \
        .getOrCreate()

    sqlContext = SQLContext(spark)
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
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
    # defined the StructType of vertexSchema
    vertexSchema = StructType([StructField("id", StringType(), True),
                               StructField("Borough", StringType(), True),
                               StructField("Zone", StringType(), True),
                               StructField("service_zone", StringType(), True)])

    # defined the StructType of edgeSchema
    edgeSchema = StructType([StructField("src", StringType(), True),
                               StructField("dst", StringType(), True)])

    # used 'pickup_location' and 'dropoff_location' columns of rideshare_data.csv as 'src' and 'dst' information
    edgesDF = spark.read.format("csv") \
                   .option("header", True) \
                   .csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv") \
                   .select(col("pickup_location"), col("dropoff_location"))
    
    # Apply edgeSchema schema on the Dataframe edgesDF
    edgesDF = spark.createDataFrame(edgesDF.rdd, schema=edgeSchema)
                   

    # used 'LocationID', 'Borough', 'Zone' and 'service_zone' columns of taxi_zone_lookup.csv to define vertex information of vertexSchema
    verticesDF = spark.read.format("csv") \
                      .option("header", True) \
                      .schema(vertexSchema) \
                      .csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv")
    

    
    
    # Solution 2
    # displayed 10 sample from the vertices and edges DF in tabular format using show API, used 'truncate = False' to avoid shortening of large strings
    print(verticesDF.show(10, truncate = False))
    print(edgesDF.show(10))



    
    # Solution 3
    # created a graph using the verticesDF as vertices and edgesDF as edges
    graph = GraphFrame(verticesDF, edgesDF)
    # graph.vertices.show(10, truncate = False)
    # graph.edges.show(10, truncate = False)

    # displayed 10 samples of the graph DataFrame with vertices ‘src’, ‘dst’ and edge 'edge' in tabular format using show API
    graph.triplets.show(10, truncate = False)

    

    # Solution 4
    # used find function of graphframes to identify vertices with the same Borough and same service_zone
    # stored the modified DF in same_Borough_service_zone
    # displayed the count of connected vertices with the same Borough and same service_zone
    same_Borough_service_zone = graph.find("(a)-[]->(b)") \
                                     .filter("a.Borough = b.Borough") \
                                     .filter("a.service_zone = b.service_zone")
    print ("count: %d" % same_Borough_service_zone.count())

    # used select API to query same_Borough_service_zone columns id'(src), 'id'(dst), 'Borough', and 'service_zone'
    # stored the selected columns in 'selected' schema
    # displayed 10 samples from the DF in tabular format using show API, used 'truncate = False' to avoid shortening of large strings
    selected = same_Borough_service_zone.select("a.id", "b.id", "b.Borough", "b.service_zone")
    selected.show(10, truncate=False)

    

    # Solution 5
    # used pageRank function to perform ranking of vertices of the 'graph' until it converged to the specified 'tolerance'
    # used vertices.select to select columns 'id and 'pagerank', stored the columns in new schema 'result'
    # used orderBy API to sort the result based on decreasing value of pagerank
    # displayed the top 5 results in tabular format using show function
    results = graph.pageRank(resetProbability=0.17, tol=0.01)
    result = results.vertices.select("id", "pagerank") \
                    .orderBy("pagerank", ascending = False)
    print(result.show(5))

    # closed the current SparkSession
    spark.stop()


