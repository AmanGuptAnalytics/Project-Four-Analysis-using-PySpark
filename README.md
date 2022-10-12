# Project-Four-Analysis-using-PySpark
The taxi data used in project three is ingested in this project and analysed using Spark SQL, connecting Spark to GCS bucket and Running the Spark job on Data proc.

# Project-Four-Analysis-using-PySpark

This modules used in this project are :
+   Data Ingestion using Bash
+   Querying Data with Spark
+   Jupyter notebooks 
+   Spark on Google Dataproc


## 1. [Data Ingestion using Bash](https://github.com/AmanGuptAnalytics/Project-Four-Analysis-using-PySpark/blob/main/1.%20Download_data.sh) 

The Data to be used in this project is on this website [NYC Taxi & Limousine Commision](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page). From this we ingest green and yellow taxi data 
of years 2019 and 2020. The following BASH commands are used for this purpose


## 2. [Querying Data with Spark](https://github.com/AmanGuptAnalytics/Project-Four-Analysis-using-PySpark/blob/main/2.Using%20SQL%20queries.ipynb)

To run the SQL queries in Spark do the following steps:

+   To do this we created a spark session. 
+   Read the data from local storage to form two spark dataframes
+   The data frames are Joined by using UNION ALL
+   And the following SQL queries

```
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
```

## 3. [Using Joins with Spark](https://github.com/AmanGuptAnalytics/Project-Four-Analysis-using-PySpark/blob/main/3.%20Grouping_by_joins.ipynb)

The next thing we did is to join the data from two taxis i.e. Green and Yellow and then select the data pertaining to particular zones so we joined the combined data with zones table as given in query below. The Details of how this is done is given [here](https://github.com/AmanGuptAnalytics/Project-Four-Analysis-using-PySpark/blob/main/3.%20Grouping_by_joins.ipynb).



```
df_join = df_green_revenue_tmp.join(df_yellow_revenue_tmp, on=['hour', 'zone'], how='outer')

# And then this join 

df_result = df_join.join(df_zones, df_join.zone == df_zones.LocationID)
```
