# Databricks notebook source
!pip install fsspec s3fs boto3
!pip install pyarrow fastparquet
!pip install seaborn


pip install --upgrade pandas


import pandas as pd
import pyarrow
import fastparquet
import seaborn as sns


fhv_df1 = pd.read_parquet("https://my-project-nm.s3.us-east-2.amazonaws.com/landing/fhvhv_tripdata_2023-01.parquet")


# column names and data types for each column
print(fhv_df1.info())


# summary statistics of each column
fhv_df1.describe()


# number of observations
fhv_df1.count()


# list of variable/ column names
variable_list = list(fhv_df1)
print(variable_list)


# null values?
# Which columns have nulls or NaN ?
fhv_df1.columns[fhv_df1.isnull().any()].tolist()
# How many rows have nulls?
print("Rows with null values:", fhv_df1.isnull().any(axis=1).sum())


fhv_df2 = pd.read_parquet("https://my-project-nm.s3.us-east-2.amazonaws.com/landing/fhvhv_tripdata_2019-02.parquet")


# column names and data types for each column
print(fhv_df2.info())


# summary statistics of each column
fhv_df2.describe()


# number of observations
fhv_df2.count()


# list of variable/ column names
variable_list = list(fhv_df2)
print(variable_list)


# null values?
# Which columns have nulls or NaN ?
fhv_df2.columns[fhv_df2.isnull().any()].tolist()
# How many rows have nulls?
print("Rows with null values:", fhv_df2.isnull().any(axis=1).sum())


# convert variable trip_miles from float to integer 
fhv_df2['trip_miles'] = fhv_df2['trip_miles'].fillna(0).astype(int)


# make sure that the changed dtype is stored
print(fhv_df2.info())


# make a boxplot showing the trip_miles
sns.boxplot(x=fhv_df2["trip_miles"])


# make a boxplot without outliers for trip_miles
sns.boxplot(x=fhv_df2["trip_miles"], showfliers= False)
