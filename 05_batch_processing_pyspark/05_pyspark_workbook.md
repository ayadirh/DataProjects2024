```python
import pyspark
```


```python
pyspark.__file__
```




    '/home/pusang/spark/spark-3.3.2-bin-hadoop3/python/pyspark/__init__.py'




```python
from pyspark.sql import SparkSession
```


```python
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
```


```python
spark.version
```




    '3.3.2'




```python
# homework 
df = spark.read \
    .option("header", "true") \
    .csv('fhv_tripdata_2019-10.csv.gz')
```


```python
df.show()
```

    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+
    |dispatching_base_num|    pickup_datetime|   dropOff_datetime|PUlocationID|DOlocationID|SR_Flag|Affiliated_base_number|
    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+
    |              B00009|2019-10-01 00:23:00|2019-10-01 00:35:00|         264|         264|   null|                B00009|
    |              B00013|2019-10-01 00:11:29|2019-10-01 00:13:22|         264|         264|   null|                B00013|
    |              B00014|2019-10-01 00:11:43|2019-10-01 00:37:20|         264|         264|   null|                B00014|
    |              B00014|2019-10-01 00:56:29|2019-10-01 00:57:47|         264|         264|   null|                B00014|
    |              B00014|2019-10-01 00:23:09|2019-10-01 00:28:27|         264|         264|   null|                B00014|
    |     B00021         |2019-10-01 00:00:48|2019-10-01 00:07:12|         129|         129|   null|       B00021         |
    |     B00021         |2019-10-01 00:47:23|2019-10-01 00:53:25|          57|          57|   null|       B00021         |
    |     B00021         |2019-10-01 00:10:06|2019-10-01 00:19:50|         173|         173|   null|       B00021         |
    |     B00021         |2019-10-01 00:51:37|2019-10-01 01:06:14|         226|         226|   null|       B00021         |
    |     B00021         |2019-10-01 00:28:23|2019-10-01 00:34:33|          56|          56|   null|       B00021         |
    |     B00021         |2019-10-01 00:31:17|2019-10-01 00:51:52|          82|          82|   null|       B00021         |
    |              B00037|2019-10-01 00:07:41|2019-10-01 00:15:23|         264|          71|   null|                B00037|
    |              B00037|2019-10-01 00:13:38|2019-10-01 00:25:51|         264|          39|   null|                B00037|
    |              B00037|2019-10-01 00:42:40|2019-10-01 00:53:47|         264|         188|   null|                B00037|
    |              B00037|2019-10-01 00:58:46|2019-10-01 01:10:11|         264|          91|   null|                B00037|
    |              B00037|2019-10-01 00:09:49|2019-10-01 00:14:37|         264|          71|   null|                B00037|
    |              B00037|2019-10-01 00:22:35|2019-10-01 00:36:53|         264|          35|   null|                B00037|
    |              B00037|2019-10-01 00:54:27|2019-10-01 01:03:37|         264|          61|   null|                B00037|
    |              B00037|2019-10-01 00:08:12|2019-10-01 00:28:47|         264|         198|   null|                B00037|
    |              B00053|2019-10-01 00:05:24|2019-10-01 00:53:03|         264|         264|   null|                  #N/A|
    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+
    only showing top 20 rows
    



```python
df.head(5)
```




    [Row(dispatching_base_num='B00009', pickup_datetime='2019-10-01 00:23:00', dropOff_datetime='2019-10-01 00:35:00', PUlocationID='264', DOlocationID='264', SR_Flag=None, Affiliated_base_number='B00009'),
     Row(dispatching_base_num='B00013', pickup_datetime='2019-10-01 00:11:29', dropOff_datetime='2019-10-01 00:13:22', PUlocationID='264', DOlocationID='264', SR_Flag=None, Affiliated_base_number='B00013'),
     Row(dispatching_base_num='B00014', pickup_datetime='2019-10-01 00:11:43', dropOff_datetime='2019-10-01 00:37:20', PUlocationID='264', DOlocationID='264', SR_Flag=None, Affiliated_base_number='B00014'),
     Row(dispatching_base_num='B00014', pickup_datetime='2019-10-01 00:56:29', dropOff_datetime='2019-10-01 00:57:47', PUlocationID='264', DOlocationID='264', SR_Flag=None, Affiliated_base_number='B00014'),
     Row(dispatching_base_num='B00014', pickup_datetime='2019-10-01 00:23:09', dropOff_datetime='2019-10-01 00:28:27', PUlocationID='264', DOlocationID='264', SR_Flag=None, Affiliated_base_number='B00014')]




```python
df.schema
```




    StructType([StructField('dispatching_base_num', StringType(), True), StructField('pickup_datetime', StringType(), True), StructField('dropOff_datetime', StringType(), True), StructField('PUlocationID', StringType(), True), StructField('DOlocationID', StringType(), True), StructField('SR_Flag', StringType(), True), StructField('Affiliated_base_number', StringType(), True)])




```python
import gzip

input_file = 'fhv_tripdata_2019-10.csv.gz'
output_file = 'fhv_tripdata_2019-10.csv'

with gzip.open(input_file, 'rb') as f_in:
    with open(output_file, 'wb') as f_out:
        f_out.write(f_in.read())

```


```python
!head -n 1001 fhv_tripdata_2019-10.csv > head.csv
```


```python
import gzip
import pandas as pd
```


```python
# with gzip.open('head.csv.gz', 'rt') as f:  # Open the compressed file for reading text
df_pandas = pd.read_csv('head.csv')
    
df_pandas.dtypes
```




    dispatching_base_num       object
    pickup_datetime            object
    dropOff_datetime           object
    PUlocationID              float64
    DOlocationID              float64
    SR_Flag                   float64
    Affiliated_base_number     object
    dtype: object




```python
# Assuming 'spark' is your SparkSession object
# import pandas as pd
# pd.DataFrame.iteritems = pd.DataFrame.items
# spark_df = df_pandas.withColumn("Affiliated_base_number", col("Affiliated_base_number").cast(StringType()))

spark_df = spark.read \
    .option("header", "true") \
    .csv('head.csv')

# spark_df=spark.read.option("header", "true") \
#                 .createDataFrame(df_pandas)

# Show the Spark DataFrame
# spark_df.show()

```


```python
#show
spark_df.show
```




    <bound method DataFrame.show of DataFrame[dispatching_base_num: string, pickup_datetime: string, dropOff_datetime: string, PUlocationID: string, DOlocationID: string, SR_Flag: string, Affiliated_base_number: string]>




```python
#schema
spark_df.schema
```




    StructType([StructField('dispatching_base_num', StringType(), True), StructField('pickup_datetime', StringType(), True), StructField('dropOff_datetime', StringType(), True), StructField('PUlocationID', StringType(), True), StructField('DOlocationID', StringType(), True), StructField('SR_Flag', StringType(), True), StructField('Affiliated_base_number', StringType(), True)])




```python
from pyspark.sql import types
```


```python
schema = types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True), 
    types.StructField('pickup_datetime', types.TimestampType(), True), 
    types.StructField('dropOff_datetime',types.TimestampType(), True), 
    types.StructField('PUlocationID', types.IntegerType(), True), 
    types.StructField('DOlocationID', types.IntegerType(), True), 
    types.StructField('SR_Flag', types.StringType(), True), 
    types.StructField('Affiliated_base_number', types.StringType(), True)
])
```


```python
#reading a csv file with schema
spark_df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('head.csv')
```


```python
spark_df.head(10)
```




    [Row(dispatching_base_num='B00009', pickup_datetime=datetime.datetime(2019, 10, 1, 0, 23), dropOff_datetime=datetime.datetime(2019, 10, 1, 0, 35), PUlocationID=264, DOlocationID=264, SR_Flag=None, Affiliated_base_number='B00009'),
     Row(dispatching_base_num='B00013', pickup_datetime=datetime.datetime(2019, 10, 1, 0, 11, 29), dropOff_datetime=datetime.datetime(2019, 10, 1, 0, 13, 22), PUlocationID=264, DOlocationID=264, SR_Flag=None, Affiliated_base_number='B00013'),
     Row(dispatching_base_num='B00014', pickup_datetime=datetime.datetime(2019, 10, 1, 0, 11, 43), dropOff_datetime=datetime.datetime(2019, 10, 1, 0, 37, 20), PUlocationID=264, DOlocationID=264, SR_Flag=None, Affiliated_base_number='B00014'),
     Row(dispatching_base_num='B00014', pickup_datetime=datetime.datetime(2019, 10, 1, 0, 56, 29), dropOff_datetime=datetime.datetime(2019, 10, 1, 0, 57, 47), PUlocationID=264, DOlocationID=264, SR_Flag=None, Affiliated_base_number='B00014'),
     Row(dispatching_base_num='B00014', pickup_datetime=datetime.datetime(2019, 10, 1, 0, 23, 9), dropOff_datetime=datetime.datetime(2019, 10, 1, 0, 28, 27), PUlocationID=264, DOlocationID=264, SR_Flag=None, Affiliated_base_number='B00014'),
     Row(dispatching_base_num='B00021         ', pickup_datetime=datetime.datetime(2019, 10, 1, 0, 0, 48), dropOff_datetime=datetime.datetime(2019, 10, 1, 0, 7, 12), PUlocationID=129, DOlocationID=129, SR_Flag=None, Affiliated_base_number='B00021         '),
     Row(dispatching_base_num='B00021         ', pickup_datetime=datetime.datetime(2019, 10, 1, 0, 47, 23), dropOff_datetime=datetime.datetime(2019, 10, 1, 0, 53, 25), PUlocationID=57, DOlocationID=57, SR_Flag=None, Affiliated_base_number='B00021         '),
     Row(dispatching_base_num='B00021         ', pickup_datetime=datetime.datetime(2019, 10, 1, 0, 10, 6), dropOff_datetime=datetime.datetime(2019, 10, 1, 0, 19, 50), PUlocationID=173, DOlocationID=173, SR_Flag=None, Affiliated_base_number='B00021         '),
     Row(dispatching_base_num='B00021         ', pickup_datetime=datetime.datetime(2019, 10, 1, 0, 51, 37), dropOff_datetime=datetime.datetime(2019, 10, 1, 1, 6, 14), PUlocationID=226, DOlocationID=226, SR_Flag=None, Affiliated_base_number='B00021         '),
     Row(dispatching_base_num='B00021         ', pickup_datetime=datetime.datetime(2019, 10, 1, 0, 28, 23), dropOff_datetime=datetime.datetime(2019, 10, 1, 0, 34, 33), PUlocationID=56, DOlocationID=56, SR_Flag=None, Affiliated_base_number='B00021         ')]




```python
#having a single large file maynot be a efficient approach, when you have multiple Spark executors. Thus repartitioning the file to server the multiple executors
spark_df.repartition(24)
```




    DataFrame[dispatching_base_num: string, pickup_datetime: timestamp, dropOff_datetime: timestamp, PUlocationID: int, DOlocationID: int, SR_Flag: string, Affiliated_base_number: string]




```python
#which is a lazy command which does not executed immediately until the output is assigned with other actions
spark_df = spark_df.repartition(24)
```


```python
spark_df.write.parquet('demo/head/')
#parition gets executed now
```


    ---------------------------------------------------------------------------

    AnalysisException                         Traceback (most recent call last)

    <ipython-input-23-918f3ae74ab5> in <module>
    ----> 1 spark_df.write.parquet('demo/head/')
          2 #parition gets executed now


    ~/spark/spark-3.3.2-bin-hadoop3/python/pyspark/sql/readwriter.py in parquet(self, path, mode, partitionBy, compression)
       1138             self.partitionBy(partitionBy)
       1139         self._set_opts(compression=compression)
    -> 1140         self._jwrite.parquet(path)
       1141 
       1142     def text(


    ~/.local/lib/python3.8/site-packages/py4j/java_gateway.py in __call__(self, *args)
       1320 
       1321         answer = self.gateway_client.send_command(command)
    -> 1322         return_value = get_return_value(
       1323             answer, self.gateway_client, self.target_id, self.name)
       1324 


    ~/spark/spark-3.3.2-bin-hadoop3/python/pyspark/sql/utils.py in deco(*a, **kw)
        194                 # Hide where the exception came from that shows a non-Pythonic
        195                 # JVM exception message.
    --> 196                 raise converted from None
        197             else:
        198                 raise


    AnalysisException: path file:/home/pusang/data-engineering-zoomcamp/05-batch/notebooks/demo/head already exists.



```python
#reading a csv file with schema for homework question 2
hw_df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('fhv_tripdata_2019-10.csv')
```


```python
hw_df.head(10)
```




    [Row(dispatching_base_num='B00009', pickup_datetime=datetime.datetime(2019, 10, 1, 0, 23), dropOff_datetime=datetime.datetime(2019, 10, 1, 0, 35), PUlocationID=264, DOlocationID=264, SR_Flag=None, Affiliated_base_number='B00009'),
     Row(dispatching_base_num='B00013', pickup_datetime=datetime.datetime(2019, 10, 1, 0, 11, 29), dropOff_datetime=datetime.datetime(2019, 10, 1, 0, 13, 22), PUlocationID=264, DOlocationID=264, SR_Flag=None, Affiliated_base_number='B00013'),
     Row(dispatching_base_num='B00014', pickup_datetime=datetime.datetime(2019, 10, 1, 0, 11, 43), dropOff_datetime=datetime.datetime(2019, 10, 1, 0, 37, 20), PUlocationID=264, DOlocationID=264, SR_Flag=None, Affiliated_base_number='B00014'),
     Row(dispatching_base_num='B00014', pickup_datetime=datetime.datetime(2019, 10, 1, 0, 56, 29), dropOff_datetime=datetime.datetime(2019, 10, 1, 0, 57, 47), PUlocationID=264, DOlocationID=264, SR_Flag=None, Affiliated_base_number='B00014'),
     Row(dispatching_base_num='B00014', pickup_datetime=datetime.datetime(2019, 10, 1, 0, 23, 9), dropOff_datetime=datetime.datetime(2019, 10, 1, 0, 28, 27), PUlocationID=264, DOlocationID=264, SR_Flag=None, Affiliated_base_number='B00014'),
     Row(dispatching_base_num='B00021         ', pickup_datetime=datetime.datetime(2019, 10, 1, 0, 0, 48), dropOff_datetime=datetime.datetime(2019, 10, 1, 0, 7, 12), PUlocationID=129, DOlocationID=129, SR_Flag=None, Affiliated_base_number='B00021         '),
     Row(dispatching_base_num='B00021         ', pickup_datetime=datetime.datetime(2019, 10, 1, 0, 47, 23), dropOff_datetime=datetime.datetime(2019, 10, 1, 0, 53, 25), PUlocationID=57, DOlocationID=57, SR_Flag=None, Affiliated_base_number='B00021         '),
     Row(dispatching_base_num='B00021         ', pickup_datetime=datetime.datetime(2019, 10, 1, 0, 10, 6), dropOff_datetime=datetime.datetime(2019, 10, 1, 0, 19, 50), PUlocationID=173, DOlocationID=173, SR_Flag=None, Affiliated_base_number='B00021         '),
     Row(dispatching_base_num='B00021         ', pickup_datetime=datetime.datetime(2019, 10, 1, 0, 51, 37), dropOff_datetime=datetime.datetime(2019, 10, 1, 1, 6, 14), PUlocationID=226, DOlocationID=226, SR_Flag=None, Affiliated_base_number='B00021         '),
     Row(dispatching_base_num='B00021         ', pickup_datetime=datetime.datetime(2019, 10, 1, 0, 28, 23), dropOff_datetime=datetime.datetime(2019, 10, 1, 0, 34, 33), PUlocationID=56, DOlocationID=56, SR_Flag=None, Affiliated_base_number='B00021         ')]




```python
# repartitioning the data into 6 and saving in parquet
hw_df = hw_df.repartition(6)
hw_df.write.parquet('hw/fhv/')
```


    ---------------------------------------------------------------------------

    AnalysisException                         Traceback (most recent call last)

    <ipython-input-26-133f3fa0539a> in <module>
          1 # repartitioning the data into 6 and saving in parquet
          2 hw_df = hw_df.repartition(6)
    ----> 3 hw_df.write.parquet('hw/fhv/')
    

    ~/spark/spark-3.3.2-bin-hadoop3/python/pyspark/sql/readwriter.py in parquet(self, path, mode, partitionBy, compression)
       1138             self.partitionBy(partitionBy)
       1139         self._set_opts(compression=compression)
    -> 1140         self._jwrite.parquet(path)
       1141 
       1142     def text(


    ~/.local/lib/python3.8/site-packages/py4j/java_gateway.py in __call__(self, *args)
       1320 
       1321         answer = self.gateway_client.send_command(command)
    -> 1322         return_value = get_return_value(
       1323             answer, self.gateway_client, self.target_id, self.name)
       1324 


    ~/spark/spark-3.3.2-bin-hadoop3/python/pyspark/sql/utils.py in deco(*a, **kw)
        194                 # Hide where the exception came from that shows a non-Pythonic
        195                 # JVM exception message.
    --> 196                 raise converted from None
        197             else:
        198                 raise


    AnalysisException: path file:/home/pusang/data-engineering-zoomcamp/05-batch/notebooks/hw/fhv already exists.



```python
#reading parquet file in Spark
read_df = spark.read.parquet('hw/fhv/')
```


```python
read_df.show()
```

    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+
    |dispatching_base_num|    pickup_datetime|   dropOff_datetime|PUlocationID|DOlocationID|SR_Flag|Affiliated_base_number|
    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+
    |              B02735|2019-10-03 22:35:28|2019-10-03 22:41:01|         264|         259|   null|                B02682|
    |              B01145|2019-10-01 10:55:00|2019-10-01 10:58:18|         264|         174|   null|                B02864|
    |              B02569|2019-10-04 07:01:37|2019-10-04 07:37:50|         193|         100|   null|                B02759|
    |              B00727|2019-10-02 14:10:31|2019-10-02 14:22:31|         264|         264|   null|                B00727|
    |              B00628|2019-10-03 01:28:47|2019-10-03 02:02:31|         261|         191|   null|                B00628|
    |              B01051|2019-10-01 11:52:26|2019-10-01 12:04:22|         264|         168|   null|                B01051|
    |              B02531|2019-10-04 07:00:00|2019-10-04 08:00:00|           9|          92|   null|                B02531|
    |              B00821|2019-10-01 14:55:33|2019-10-01 15:03:35|         264|         258|   null|                B00821|
    |              B00310|2019-10-03 19:24:01|2019-10-03 19:47:57|         264|         213|   null|                B02847|
    |              B03160|2019-10-01 20:09:00|2019-10-01 20:22:00|          72|          76|   null|                B02877|
    |              B01239|2019-10-01 14:35:01|2019-10-01 14:59:38|         264|         147|   null|                B02682|
    |              B01454|2019-10-02 20:25:39|2019-10-02 20:31:03|         264|          35|   null|                B01454|
    |              B01328|2019-10-04 11:03:00|2019-10-04 11:21:00|          20|          78|   null|                B02534|
    |              B01308|2019-10-04 10:51:00|2019-10-04 11:13:00|         264|         264|   null|                B01308|
    |              B01717|2019-10-01 05:04:55|2019-10-01 05:13:50|         264|          74|   null|                B01717|
    |              B00972|2019-10-04 12:38:18|2019-10-04 12:56:02|          44|         109|   null|                B00972|
    |              B01437|2019-10-01 02:30:24|2019-10-01 03:01:08|         264|         121|   null|                B03227|
    |              B00477|2019-10-04 07:48:52|2019-10-04 07:49:00|         237|         236|   null|                B00477|
    |              B02729|2019-10-03 20:40:00|2019-10-03 20:45:37|         264|         264|   null|                B02729|
    |              B02739|2019-10-04 09:22:01|2019-10-04 09:34:42|         264|         198|   null|                B02739|
    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+
    only showing top 20 rows
    



```python
read_df.printSchema()
```

    root
     |-- dispatching_base_num: string (nullable = true)
     |-- pickup_datetime: timestamp (nullable = true)
     |-- dropOff_datetime: timestamp (nullable = true)
     |-- PUlocationID: integer (nullable = true)
     |-- DOlocationID: integer (nullable = true)
     |-- SR_Flag: string (nullable = true)
     |-- Affiliated_base_number: string (nullable = true)
    



```python
read_df.select('pickup_datetime', 'dropOff_datetime', 'PUlocationID', 'DOlocationID')
```




    DataFrame[pickup_datetime: timestamp, dropOff_datetime: timestamp, PUlocationID: int, DOlocationID: int]




```python
# filtering data
from pyspark.sql.functions import col
# read_df.filter((col("pickup_datetime").day == 15) & (col("pickup_datetime").month == 10))
# .select('pickup_datetime', 'dropOff_datetime', 'PUlocationID', 'DOlocationID') \

# Count the total number of rows where pickup_datetime is on the 15th of October
count_oct_15 = read_df.filter((col("pickup_datetime").substr(9, 2) == "15") & (col("pickup_datetime").substr(6, 2) == "10")).count()

    
```


```python
print("The total count for Oct 15 is", count_oct_15)
```

    The total count for Oct 15 is 62610



```python
from pyspark.sql import functions as F
```


```python
read_df.withColumn('pickup_date', F.to_date(read_df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(read_df.dropOff_datetime)) \
    .select('pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()
```

    +-----------+------------+------------+------------+
    |pickup_date|dropoff_date|PULocationID|DOLocationID|
    +-----------+------------+------------+------------+
    | 2019-10-03|  2019-10-03|         264|         259|
    | 2019-10-01|  2019-10-01|         264|         174|
    | 2019-10-04|  2019-10-04|         193|         100|
    | 2019-10-02|  2019-10-02|         264|         264|
    | 2019-10-03|  2019-10-03|         261|         191|
    | 2019-10-01|  2019-10-01|         264|         168|
    | 2019-10-04|  2019-10-04|           9|          92|
    | 2019-10-01|  2019-10-01|         264|         258|
    | 2019-10-03|  2019-10-03|         264|         213|
    | 2019-10-01|  2019-10-01|          72|          76|
    | 2019-10-01|  2019-10-01|         264|         147|
    | 2019-10-02|  2019-10-02|         264|          35|
    | 2019-10-04|  2019-10-04|          20|          78|
    | 2019-10-04|  2019-10-04|         264|         264|
    | 2019-10-01|  2019-10-01|         264|          74|
    | 2019-10-04|  2019-10-04|          44|         109|
    | 2019-10-01|  2019-10-01|         264|         121|
    | 2019-10-04|  2019-10-04|         237|         236|
    | 2019-10-03|  2019-10-03|         264|         264|
    | 2019-10-04|  2019-10-04|         264|         198|
    +-----------+------------+------------+------------+
    only showing top 20 rows
    



```python
read_df.printSchema()
```

    root
     |-- dispatching_base_num: string (nullable = true)
     |-- pickup_datetime: timestamp (nullable = true)
     |-- dropOff_datetime: timestamp (nullable = true)
     |-- PUlocationID: integer (nullable = true)
     |-- DOlocationID: integer (nullable = true)
     |-- SR_Flag: string (nullable = true)
     |-- Affiliated_base_number: string (nullable = true)
    



```python
filtered_df = read_df.withColumn('pickup_date', F.to_date(read_df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(read_df.dropOff_datetime)) \
.select('pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') 
    
filtered_df.printSchema()
```

    root
     |-- pickup_date: date (nullable = true)
     |-- dropoff_date: date (nullable = true)
     |-- PULocationID: integer (nullable = true)
     |-- DOLocationID: integer (nullable = true)
    



```python
counter = filtered_df.filter(filtered_df.pickup_date == '2019-10-15').count()
print(counter)
```

    62610



```python
set(read_df.columns)
```




    {'Affiliated_base_number',
     'DOlocationID',
     'PUlocationID',
     'SR_Flag',
     'dispatching_base_num',
     'dropOff_datetime',
     'pickup_datetime'}




```python
from pyspark.sql.functions import expr

```


```python
df_with_diff_hours = read_df.withColumn("time_difference_hours", expr("(unix_timestamp(dropOff_datetime) - unix_timestamp(pickup_datetime))/3600"))
```


```python
# Show the result
df_with_diff_hours.show()
```

    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+---------------------+
    |dispatching_base_num|    pickup_datetime|   dropOff_datetime|PUlocationID|DOlocationID|SR_Flag|Affiliated_base_number|time_difference_hours|
    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+---------------------+
    |              B02735|2019-10-03 22:35:28|2019-10-03 22:41:01|         264|         259|   null|                B02682|               0.0925|
    |              B01145|2019-10-01 10:55:00|2019-10-01 10:58:18|         264|         174|   null|                B02864|                0.055|
    |              B02569|2019-10-04 07:01:37|2019-10-04 07:37:50|         193|         100|   null|                B02759|   0.6036111111111111|
    |              B00727|2019-10-02 14:10:31|2019-10-02 14:22:31|         264|         264|   null|                B00727|                  0.2|
    |              B00628|2019-10-03 01:28:47|2019-10-03 02:02:31|         261|         191|   null|                B00628|   0.5622222222222222|
    |              B01051|2019-10-01 11:52:26|2019-10-01 12:04:22|         264|         168|   null|                B01051|   0.1988888888888889|
    |              B02531|2019-10-04 07:00:00|2019-10-04 08:00:00|           9|          92|   null|                B02531|                  1.0|
    |              B00821|2019-10-01 14:55:33|2019-10-01 15:03:35|         264|         258|   null|                B00821|   0.1338888888888889|
    |              B00310|2019-10-03 19:24:01|2019-10-03 19:47:57|         264|         213|   null|                B02847|   0.3988888888888889|
    |              B03160|2019-10-01 20:09:00|2019-10-01 20:22:00|          72|          76|   null|                B02877|  0.21666666666666667|
    |              B01239|2019-10-01 14:35:01|2019-10-01 14:59:38|         264|         147|   null|                B02682|   0.4102777777777778|
    |              B01454|2019-10-02 20:25:39|2019-10-02 20:31:03|         264|          35|   null|                B01454|                 0.09|
    |              B01328|2019-10-04 11:03:00|2019-10-04 11:21:00|          20|          78|   null|                B02534|                  0.3|
    |              B01308|2019-10-04 10:51:00|2019-10-04 11:13:00|         264|         264|   null|                B01308|  0.36666666666666664|
    |              B01717|2019-10-01 05:04:55|2019-10-01 05:13:50|         264|          74|   null|                B01717|   0.1486111111111111|
    |              B00972|2019-10-04 12:38:18|2019-10-04 12:56:02|          44|         109|   null|                B00972|  0.29555555555555557|
    |              B01437|2019-10-01 02:30:24|2019-10-01 03:01:08|         264|         121|   null|                B03227|   0.5122222222222222|
    |              B00477|2019-10-04 07:48:52|2019-10-04 07:49:00|         237|         236|   null|                B00477| 0.002222222222222...|
    |              B02729|2019-10-03 20:40:00|2019-10-03 20:45:37|         264|         264|   null|                B02729|  0.09361111111111112|
    |              B02739|2019-10-04 09:22:01|2019-10-04 09:34:42|         264|         198|   null|                B02739|  0.21138888888888888|
    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+---------------------+
    only showing top 20 rows
    



```python
from pyspark.sql.functions import desc

# Sort the DataFrame in descending order based on the desired column
df_sorted = df_with_diff_hours.orderBy(desc("time_difference_hours"))

# Limit the result to 1 row
df_top_row = df_sorted.limit(1)

# Show the output
df_top_row.show()
```

    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+---------------------+
    |dispatching_base_num|    pickup_datetime|   dropOff_datetime|PUlocationID|DOlocationID|SR_Flag|Affiliated_base_number|time_difference_hours|
    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+---------------------+
    |              B02832|2019-10-11 18:00:00|2091-10-11 18:30:00|         264|         264|   null|                B02832|             631152.5|
    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+---------------------+
    



```python
df_sorted.createOrReplaceTempView('sorted_trip')

```


```python
spark.sql("SELECT count(*) from sorted_trip;").show()
```

    +--------+
    |count(1)|
    +--------+
    | 1897493|
    +--------+
    



```python
# taxi lookup csv
# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
df_taxi_lookup = spark.read \
    .option("header", "true") \
    .csv('taxi+_zone_lookup.csv')
```


```python
df_taxi_lookup.printSchema()
```

    root
     |-- LocationID: string (nullable = true)
     |-- Borough: string (nullable = true)
     |-- Zone: string (nullable = true)
     |-- service_zone: string (nullable = true)
    



```python
# Specify the column names to join on
join_condition = df_taxi_lookup["LocationID"] == read_df["PUlocationID"]
df_join = read_df.join(df_taxi_lookup, join_condition, how='inner')
```


```python
df_join.printSchema()
```

    root
     |-- dispatching_base_num: string (nullable = true)
     |-- pickup_datetime: timestamp (nullable = true)
     |-- dropOff_datetime: timestamp (nullable = true)
     |-- PUlocationID: integer (nullable = true)
     |-- DOlocationID: integer (nullable = true)
     |-- SR_Flag: string (nullable = true)
     |-- Affiliated_base_number: string (nullable = true)
     |-- LocationID: string (nullable = true)
     |-- Borough: string (nullable = true)
     |-- Zone: string (nullable = true)
     |-- service_zone: string (nullable = true)
    



```python
df_join.createOrReplaceTempView('joined_trip')
spark.sql("SELECT Zone, count(*) AS pickup_count from joined_trip group by Zone order by pickup_count asc LIMIT 5 ;").show()
```

    +--------------------+------------+
    |                Zone|pickup_count|
    +--------------------+------------+
    |         Jamaica Bay|           1|
    |Governor's Island...|           2|
    | Green-Wood Cemetery|           5|
    |       Broad Channel|           8|
    |     Highbridge Park|          14|
    +--------------------+------------+
    



```python

```
