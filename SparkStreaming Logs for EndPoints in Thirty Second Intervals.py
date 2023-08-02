# IMPORT ALL NECESSARY AND POTENTIALLY NECESSARY ASPECTS OF PYSPARK FOR DATA TRANSFORMATION

import findspark
findspark.init()

import pyspark # only run after findspark.init()
from pyspark.sql import SparkSession, Row
from pyspark import SparkContext
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import regexp_extract
import codecs
import pandas as pd

import pyspark # only run after findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.sql('''select 'spark' as hello ''')
df.show()

# Build SparkSession and Configure Spark Warehouse and repository location
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:\\\C:\temp").appName("StructuredStreaming").\
getOrCreate()

# Read your Access Logs that are continuously being generated at the following location
lines = spark.readStream.text("C:\\temp\\access_log(1).txt")

# Parse logs format to be easily ingested into Spark DataFrame
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

# Properly format and prepare logs DataFrame with proper column aliases
logsDF = lines.select(regexp_extract('value', hostExp, 1).alias('host'),
                     regexp_extract('value', timeExp, 1).alias('timestamp'),
                     regexp_extract('value', generalExp, 1).alias('method'),
                     regexp_extract('value', generalExp, 2).alias('endpoint'),
                     regexp_extract('value', generalExp, 3).alias('protocol'),
                     regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                     regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

# Generate "eventTime" column that tracks current timestamp of incoming files
logsDFTime = logsDF.withColumn("eventTime", func.current_timestamp())

# Group all endPoints by eventTime in the past thirty seconds, and count total endPoints during that time frame
endpointsCount = logsDFTime.groupBy(func.window(func.col("eventTime"), \
                                               "30 seconds", "10 seconds"), func.col("endpoint")).count()

# Sort endPoints by count to see most prominent endpoints during this time interval
sortedendpointsCount = endpointsCount.orderBy(func.col("count").desc())

# Run Spark Streaming session
query = ( sortedendpointsCount.writeStream.outputMode("complete").format("console")\
        .queryName("counts").start())

# Notify Spark Streaming session to await your termination
query.awaitTermination()
