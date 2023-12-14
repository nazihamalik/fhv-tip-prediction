# Databricks notebook source
# Show the confusion matrix
predictions.groupby('label').pivot('prediction').count().sort('label').show()

# Save the confusion matrix
cm = predictions.groupby('label').pivot('prediction').count().fillna(0).collect()
def calculate_recall_precision(cm):
   tn = cm[0][1] #true negative
   fp = cm[0][2] #false positive
   fn = cm[1][1] #false negative
   tp = cm[1][2] #true positive
   precision = tp / ( tp + fp )
   recall = tp / ( tp + fn )
   accuracy = ( tp + tn ) / ( tp + tn + fp + fn )
   f1_score = 2 * ( ( precision * recall ) / ( precision + recall ) )
   print("accuracy: ", accuracy)
   print("precision: ", precision)
   print("recall: ", recall)
   print("f1 score: ", f1_score)
   return accuracy, precision, recall, f1_score
  
print( calculate_recall_precision(cm) )

## SHOW ROC CURVES
# Look at the parameters for the best model that was evaluated from the grid
parammap = cv.bestModel.stages[3].extractParamMap()

for p, v in parammap.items():
   print(p, v)

# Grab the model from Stage 3 of the pipeline
mymodel = cv.bestModel.stages[3]

import matplotlib.pyplot as plt
plt.figure(figsize=(5,5))
plt.plot(mymodel.summary.roc.select('FPR').collect(),
        mymodel.summary.roc.select('TPR').collect())

plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title("ROC Curve")
plt.savefig("roc1.png")

parammap = cv.bestModel.stages[3].extractParamMap()

for p, v in parammap.items():
   print(p, v)

# Grab the model from Stage 3 of the pipeline
mymodel = cv.bestModel.stages[3]
plt.figure(figsize=(6,6))
plt.plot([0, 1], [0, 1], 'r--')
x = mymodel.summary.roc.select('FPR').collect()
y = mymodel.summary.roc.select('TPR').collect()
plt.scatter(x, y)
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title("ROC Curve")
plt.savefig("reviews_roc.png")

## CORRELATION MATRIX
correlation_columns = ['trip_miles', 'trip_time', 'base_passenger_fare', 'tips', 'day_of_month', 'month', 'request_hour', 'wait_minutes']
numeric_all = spark.read.parquet(*parquet_all).select(correlation_columns)

# Convert the numeric values to vector columns
vector_column = "correlation_features"

# Make a list of all of the numeric columns
numeric_columns = ['trip_miles', 'trip_time', 'base_passenger_fare', 'tips', 'day_of_month', 'month', 'request_hour', 'wait_minutes']

# Use a vector assembler to combine all of the numeric columns together
assembler = VectorAssembler(inputCols=numeric_columns, outputCol=vector_column)
sdf_vector = assembler.transform(numeric_all).select(vector_column)

# Create the correlation matrix, then get just the values and convert to a list
matrix = Correlation.corr(sdf_vector, vector_column).collect()[0][0]
correlation_matrix = matrix.toArray().tolist()

# Convert the correlation to a Pandas dataframe
correlation_matrix_df = pd.DataFrame(data=correlation_matrix, columns=numeric_columns, index=numeric_columns)

# Crate the plot using Seaborn
plt.figure(figsize=(16,5))
sns.heatmap(correlation_matrix_df,
           xticklabels=correlation_matrix_df.columns.values,
           yticklabels=correlation_matrix_df.columns.values,
           cmap="Greens",
           annot=True)    
plt.savefig("correlation_matrix.png")

## BAR GRAPHS OF NUMBER OF FHV ORDERS BY MONTH
# select the columns that are needed
month_columns = ["month", "request_hour", "tips"]

# create a sdf with the specified parquet files and columns
monthrequesthourtips = spark.read.parquet(*parquet_all).select(month_columns)

# Use groupby to get a count by date. Then convert to pandas dataframe
month = monthrequesthourtips.groupby("month").count().sort("month").toPandas()

# Using Pandas built-in plotting functions
# Create a bar plot using the columns order_date and count
monthplot = month.plot.bar('month','count')
# Set the x-axis and y-axis labels
monthplot.set(xlabel='Month', ylabel='Number of FHV Orders')
# Set the title
monthplot.set(title='Number of FHV Orders by Month')
monthplot.figure.set_tight_layout('tight')
# Save the plot as a PNG file
monthplot.get_figure().savefig("order_rides_by_month.png")

## BAR GRAPH OF NUMBER OF FHV RIDES BY BOROUGH
# select the columns that are needed
borough_columns = ["PULocationBorough", "DOLocationBorough", "tips"]

# create a sdf with the specified parquet files and columns
boroughtips = spark.read.parquet(*parquet_all).select(borough_columns)

# Use groupby to get a count by date. Then convert to pandas dataframe
borough = boroughtips.groupby("PULocationBorough").count().sort("PULocationBorough").toPandas()

# Using Pandas built-in plotting functions
# Create a bar plot using the columns order_date and count
boroughplot = borough.plot.bar('PULocationBorough','count')
# Set the x-axis and y-axis labels
boroughplot.set(xlabel='Borough', ylabel='Number of FHV Orders')
# Set the title
boroughplot.set(title='Number of FHV Orders by Borough')
boroughplot.figure.set_tight_layout('tight')
# Save the plot as a PNG file
boroughplot.get_figure().savefig("order_rides_by_borough.png")
