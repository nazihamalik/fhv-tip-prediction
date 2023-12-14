# Databricks notebook source
###FEATURE ENGINEERING
jan23 = spark.read.parquet("s3://my-project-nm/raw/cleaned_fhvhv_tripdata_2023-01.parquet")

### convert congestion_surcharge to a flag
jan23 = jan23.withColumn("congestion_surcharge_flag",
when(jan23.congestion_surcharge >0, "Y")
.otherwise("N")
)

### convert tolls to a flag
jan23 = jan23.withColumn("tolls_flag",
when(jan23.tolls >0, "Y")
.otherwise("N")
)

### convert airport_fee to a flag
jan23 = jan23.withColumn("airport_fee_flag",
when(jan23.airport_fee>0, "Y")
.otherwise("N")
)

### convert hvfhs_license_num to the respected fhv company
jan23 = jan23.withColumn("hvfhs_company",
when(jan23.hvfhs_license_num== "HV0005", "Lyft")
.when(jan23.hvfhs_license_num== "HV0002", "Juno")
.when(jan23.hvfhs_license_num== "HV0003", "Uber")
.when(jan23.hvfhs_license_num == "HV0004", "Via")
.otherwise("N/A")
)

### convert PULocationID to the respected borough
jan23 = jan23.withColumn("PULocationBorough",
when(jan23.PULocationID.isin(Bronx), "Bronx")
.when(jan23.PULocationID.isin(Brooklyn), "Brooklyn")
.when(jan23.PULocationID.isin(Queens), "Queens")
.when(jan23.PULocationID.isin(Manhattan), "Manhattan")
.when(jan23.PULocationID.isin(Staten_Island), "Staten Island")
.otherwise("N/A")
)
# drop records if the PULocationBorough isn't registered
PULocationCondition = col("PULocationBorough") != "N/A"
jan23 = jan23.where(PULocationCondition)

### convert DOLocation ID to the respected borough
jan23 = jan23.withColumn("DOLocationBorough",
when(jan23.DOLocationID.isin(Bronx), "Bronx")
.when(jan23.DOLocationID.isin(Brooklyn), "Brooklyn")
.when(jan23.DOLocationID.isin(Queens), "Queens")
.when(jan23.DOLocationID.isin(Manhattan), "Manhattan")
.when(jan23.DOLocationID.isin(Staten_Island), "Staten Island")
.otherwise("N/A")
)
# drop records if the DOLocationBorough isn't registered
DOLocationCondition = col("DOLocationBorough") != "N/A"
jan23 = jan23.where(DOLocationCondition)

### separate request_datetime into date and time columns
jan23 = jan23.withColumn("request_date", to_date("request_datetime", "yyyy-MM-dd"))
jan23 = jan23.withColumn("day_of_month", dayofmonth(col("request_date")))
jan23 = jan23.withColumn("day_of_week", dayofweek(col("request_date")))
def is_weekday(day):
  return "Weekday" if day in range(2, 7) else "Weekend"
spark.udf.register("is_weekday", is_weekday)
jan23 = jan23.withColumn("day_type", col("day_of_week").cast("int").alias("day_type"))
jan23 = jan23.withColumn("day_type", expr("is_weekday(day_type)"))

jan23 = jan23.withColumn("month", month(col("request_datetime")))
jan23 = jan23.withColumn("year", year(col("request_datetime")))

split_col = split(jan23['request_datetime'], ' ')
jan23 = jan23.withColumn('request_time', split_col.getItem(1))
jan23 = jan23.withColumn("request_hour", hour(col("request_time")))

## create a new column to show the wait time of a passenger
jan23 = jan23.withColumn("wait_seconds", (col("on_scene_datetime").cast("long")- col("request_datetime").cast("long")))
jan23 = jan23.withColumn("wait_minutes", col("wait_seconds") / 60)

# drop unnecessary columns
columns_to_drop= ["access_a_ride_flag", "wav_request_flag", "wav_match_flag", "bcf", "sales_tax", "shared_request_flag", "dispatching_base_num", "originating_base_num", "DOLocationID", "PULocationID", "hvfhs_license_num", "airport_fee", "tolls", "congestion_surcharge", "request_datetime", "on_scene_datetime", "pickup_datetime", "dropoff_datetime", "wait_seconds", "request_time", "request_date", "day_of_week"]
jan23 = jan23.drop(*columns_to_drop)

jan23.show(5, truncate=False)

# write the parquet to my S3 Trusted Bucket
jan23.write.parquet("s3://my-project-nm/trusted/trusted_fhvhv_tripdata_2023-01.parquet")

# read 2020 files
parquet_2020 = [("s3://my-project-nm/trusted/trusted_fhvhv_tripdata_2020-01.parquet"),
                ("s3://my-project-nm/trusted/trusted_fhvhv_tripdata_2020-02.parquet"),
                ("s3://my-project-nm/trusted/trusted_fhvhv_tripdata_2020-03.parquet"),
                ("s3://my-project-nm/trusted/trusted_fhvhv_tripdata_2020-04.parquet"),
                ("s3://my-project-nm/trusted/trusted_fhvhv_tripdata_2020-05.parquet"),
                ("s3://my-project-nm/trusted/trusted_fhvhv_tripdata_2020-06.parquet"),
                ("s3://my-project-nm/trusted/trusted_fhvhv_tripdata_2020-07.parquet"),
                ("s3://my-project-nm/trusted/trusted_fhvhv_tripdata_2020-08.parquet"),
                ("s3://my-project-nm/trusted/trusted_fhvhv_tripdata_2020-09.parquet"),
                ("s3://my-project-nm/trusted/trusted_fhvhv_tripdata_2020-10.parquet"),
                ("s3://my-project-nm/trusted/trusted_fhvhv_tripdata_2020-11.parquet"),
                ("s3://my-project-nm/trusted/trusted_fhvhv_tripdata_2020-12.parquet")]

fhv2020 = [spark.read.parquet(p) for p in parquet_2020]
combined_2020 = fhv2020[0]
for p in fhv2020[1:]:
   combined_2020 = combined_2020.union(p)

# create new columns to show our tip label
combined_2020 = combined_2020.withColumn("label20", (combined_2020.base_passenger_fare*0.20))

# COMPARING TIPS THAT ARE 20%
# Create a label. =1 if good tip, =0 otherwise
combined_2020 = combined_2020.withColumn("label", when(combined_2020.tips >= combined_2020.label20, 1.0).otherwise(0.0))

# Create an indexer for the string based columns
indexer = StringIndexer(inputCols=["shared_match_flag", "congestion_surcharge_flag", "tolls_flag", "airport_fee_flag", "hvfhs_company", "PULocationBorough", "DOLocationBorough", "day_type"],
                       outputCols=["shared_matchIndex", "congestionIndex", "tollsIndex", "airportIndex", "hvfhsIndex", "PUIndex", "DOIndex", "daytypeIndex"])

# Create an encoder for the indexes and the integer columns.
encoder = OneHotEncoder(inputCols=["shared_matchIndex", "congestionIndex", "tollsIndex", "airportIndex", "hvfhsIndex", "PUIndex", "DOIndex", "daytypeIndex", "day_of_month",  "month", "year", "request_hour"],
                       outputCols=["shared_matchVector", "congestionVector", "tollsVector", "airportVector", "hvfhsVector", "PUVector", "DOVector", "daytypeVector", "daymonthVector", "monthVector", "yearVector", "requesthourVector"], dropLast=True, handleInvalid="keep")

# Create an assembler for the individual feature vectors and the float/double columns
assembler = VectorAssembler(inputCols=["shared_matchVector", "congestionVector", "tollsVector", "airportVector", "hvfhsVector", "PUVector", "DOVector", "daytypeVector", "daymonthVector", "monthVector", "yearVector", "requesthourVector", "trip_miles", "trip_time", "base_passenger_fare", "wait_minutes"], outputCol="features")

# Create a LogisticRegression Estimator
lr = LogisticRegression()

# split the data into two subsets
trainingData, testData = combined_2020.randomSplit([0.7, 0.3])
# create the pipeline
fhv_pipe = Pipeline(stages=[indexer, encoder, assembler, lr])

# Create the parameter grid
grid = ParamGridBuilder()
grid = grid.addGrid(lr.regParam, [0.0, 0.5, 1.0])
grid = grid.addGrid(lr.elasticNetParam, [0, 1])
grid = grid.build()

print("number of models to be tested: ", len(grid))
# Create a BinaryClassificationEvaluator to evaluate how well the model works
evaluator = BinaryClassificationEvaluator(metricName="areaUnderROC")
# Create the CrossValidator using the hyperparameter grid
cv = CrossValidator(estimator=fhv_pipe,
                   estimatorParamMaps=grid,
                   evaluator=evaluator,
                   numFolds=3)
# Train the models
cv = cv.fit(trainingData)
predictions = cv.transform(testData)
auc = evaluator.evaluate(predictions)
print("auc:", auc)
