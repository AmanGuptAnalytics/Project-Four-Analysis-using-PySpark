# Project-4: Analysis-using-PySpark
The taxi data used in project three is ingested in this project and analyzed using Spark SQL, connecting Spark to the GCS bucket and Running the Spark job on Data proc.

The modules used in this project are :
+   Data Ingestion using Bash
+   Querying Data with Spark
+   Jupyter notebooks 
+   Spark on Google Dataproc


## 1. [Data Ingestion using Bash](https://github.com/AmanGuptAnalytics/Project-Four-Analysis-using-PySpark/blob/main/1.%20Download_data.sh) 

The Data to be used in this project is on this website [NYC Taxi & Limousine Commision](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page). From this, we ingest green and yellow taxi data 
for the years 2019 and 2020. The following BASH commands are used for this purpose


## 2. [Querying Data with Spark](https://github.com/AmanGuptAnalytics/Project-Four-Analysis-using-PySpark/blob/main/2.Using%20SQL%20queries.ipynb)

To run the SQL queries in Spark do the following steps:

+   To do this we created a spark session. 
+   Read the data from local storage to form two spark data frames
+   The data frames are Joined by using UNION ALL
+   And the following SQL queries

## 3. [Using Joins with Spark](https://github.com/AmanGuptAnalytics/Project-Four-Analysis-using-PySpark/blob/main/3.%20Grouping_by_joins.ipynb)

The next thing we did is to join the data from two taxis i.e. Green and Yellow and then select the data about particular zones so we joined the combined data with the zones table as given in the query below. The Details of how this is done are given [here](https://github.com/AmanGuptAnalytics/Project-Four-Analysis-using-PySpark/blob/main/3.%20Grouping_by_joins.ipynb).



```
df_join = df_green_revenue_tmp.join(df_yellow_revenue_tmp, on=['hour', 'zone'], how='outer')

# And then this join 

df_result = df_join.join(df_zones, df_join.zone == df_zones.LocationID)
```

The df_result can be queried as/when required and can be written to cloud storage or also the BQ. 


## 4.[Running Spark in the cloud](https://github.com/AmanGuptAnalytics/Project-Four-Analysis-using-PySpark/blob/main/4.Spark_GCS_connection.ipynb)

### Connecting to Google Cloud Storage

+   Uploading data to GCS:

```         
    gsutil -m cp -r pq/ gs://dtc_data_lake_de-zoomcamp-nytaxi/pq
```


+   Download the jar for connecting to GCS to any location (e.g. the lib folder):

```
gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar

```

In the [code](https://github.com/AmanGuptAnalytics/Project-Four-Analysis-using-PySpark/blob/main/4.Spark_GCS_connection.ipynb) specify the 
location of the GCS connector. As shown below.

```
conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "./lib/gcs-connector-hadoop3-2.2.5.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)
```

+   Using this configuration the Spark will be able to read from the GCS bucket as follows

```df_green = spark.read.parquet('gs://tez-bucket-spark/pq/green/*/*')```

Without defining the connector in the conf, spark shall not be able to do operations on "gs://".

+   Next step is to run SQL queries or create result tables as per the problem statement. 

## 5. [Spark using Google Dataproc](https://github.com/AmanGuptAnalytics/Project-Four-Analysis-using-PySpark/blob/main/5.Using%20Dataproc.py)

Dataproc is a managed Apache Spark and Apache Hadoop service that lets you take advantage of open-source data tools for batch processing, querying, streaming, and machine learning. Dataproc automation helps you create clusters quickly, manage them easily, and save money by turning clusters off when you don't need them.

So basically it is a managed instance that allows you to run a Hadoop or Spark job on your data.

#### **Steps involved** 

+   The Jupyter notebook script used was converted to [Python Script](https://github.com/AmanGuptAnalytics/Project-Four-Analysis-using-PySpark/blob/main/5.Using%20Dataproc.py) that can be used by Spark.
+   A data proc cluster is made and the Python script is given as an input to data proc with the arguments as defined in the script. The Green or Yellow taxi and the year, of which data is required. 
+   The Cluster is run and a job is submitted with the following arguments.

```
  --input_green=gs://tez-bucket-spark/pq/green/2021/*/
  --input_yellow=gs://tez-bucket-spark/pq/yellow/2021/*/
  --output=gs://tez-b`ucket-spark/report-2021
```
The output of this job is written as a parquet file having the result of the SQL query run. 

The output of the data procs job is as follows.

![Dataproc](https://github.com/AmanGuptAnalytics/Project-Four-Analysis-using-PySpark/blob/main/Docs/Data%20Proc.png)

This concludes Project Four Data Analysis using Spark. 
