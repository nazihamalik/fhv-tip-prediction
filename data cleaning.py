# Databricks notebook source
# To work with Amazon S3 install boto3,  s3fs
%pip install "boto3>=1.28" "s3fs>=2023.3.0"
# If your files are in Parquet format, install pyarrow and fastparquet
%pip install pyarrow fastparquet
# For visualizations, install seaborn
%pip install seaborn


Spark


import os
# To work with Amazon S3 storage, set the following variables using your AWS Access Key and Secret Key
# Set the Region to where your files are stored in S3.
access_key = '####################'
secret_key = '######################################'
# Set the environment variables so boto3 can pick them up later os.environ['AWS_ACCESS_KEY_ID'] = access_key os.environ['AWS_SECRET_ACCESS_KEY'] = secret_key encoded_secret_key = secret_key.replace("/", "%2F")
aws_region = "us-east-2"


sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3." + aws_region + ".amazonaws.com")


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, lit, when, date_format, expr, to_date, to_timestamp, date_format, split, hour, dayofweek, dayofmonth, month, year


### DATA CLEANING
# read the data from my landing folder
dec21 = spark.read.parquet("s3://my-project-nm/landing/fhvhv_tripdata_2021-12.parquet")


# drop necessary null values
columns_to_drop_null = ["request_datetime", "on_scene_datetime", "pickup_datetime", "dropoff_datetime" ]
dec21 = dec21.na.drop(subset=columns_to_drop_null)


# convert trip_time from seconds to minutes
dec21 = dec21.withColumn('trip_time', (F.col('trip_time') /60))


# drop records where trip_miles is an outlier based on previous EDA
trip_miles_condition = col("trip_miles") <= 15
dec21 = dec21.where(trip_miles_condition)


dec21.show(5)


# write the parquet to my S3 Raw Bucket
dec21.write.parquet("s3://my-project-nm/raw/cleaned_fhvhv_tripdata_2021-12.parquet")
