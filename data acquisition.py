# Databricks notebook source
#look at the individual data files in the Kaggle data set
kaggle datasets files jeffsinsel/nyc-fhvhv-data
#download individual data files onto EC2
kaggle datasets download -d jeffsinsel/nyc-fhvhv-data -f fhvhv_tripdata_2019-06.parquet
#unzip file:
unzip fhvhv_tripdata_2019-06.parquet
#copy file onto Amazon S3 Bucket named “landing”:
aws s3 cp fhvhv_tripdata_2019-06.parquet s3://my-project-nm/landing/fhvhv_tripdata_2019-06.parquet
#remove the downloaded file off of EC2 once confirming it was uploaded onto my bucket:
rm fhvhv_tripdata_2019-06.parquet
rm fhvhv_tripdata_2019-06.parquet.zip

#download individual data files onto EC2
kaggle datasets download -d jeffsinsel/nyc-fhvhv-data -f fhvhv_tripdata_2019-02.parquet
#unzip file:
unzip fhvhv_tripdata_2019-02.parquet
#copy file onto Amazon S3 Bucket named “landing”:
aws s3 cp fhvhv_tripdata_2019-02.parquet s3://my-project-nm/landing/fhvhv_tripdata_2019-02.parquet
#remove the downloaded file off of EC2 once confirming it was uploaded onto my bucket:
rm fhvhv_tripdata_2019-02.parquet
rm fhvhv_tripdata_2019-02.parquet.zip

#download individual data files onto EC2
kaggle datasets download -d jeffsinsel/nyc-fhvhv-data -f fhvhv_tripdata_2019-03.parquet
#unzip file:
unzip fhvhv_tripdata_2019-03.parquet
#copy file onto Amazon S3 Bucket named “landing”:
aws s3 cp fhvhv_tripdata_2019-03.parquet s3://my-project-nm/landing/fhvhv_tripdata_2019-03.parquet
#remove the downloaded file off of EC2 once confirming it was uploaded onto my bucket:
rm fhvhv_tripdata_2019-03.parquet
rm fhvhv_tripdata_2019-03.parquet.zip

#download the data set using curl, and with the pipe, I was able to upload it onto my S3 bucket:
curl -SL https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-01.parquet | aws s3 cp - s3://my-project-nm/landing/fhvhv_tripdata_2022-01.parquet

#download the data set using curl, and with the pipe, I was able to upload it onto my S3 bucket:
curl -SL https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2020-12.parquet | aws s3 cp - s3://my-project-nm/landing/fhvhv_tripdata_2020-12.parquet
