from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime

client = Elasticsearch([{"host": "127.0.0.1", "port": 9200}])
indexName1 = "arendator"
indexName2 = "car"

sparkSession = SparkSession.builder.appName("spark-csv").getOrCreate()
searchBody = {
    "size": 60,
    "query": {
        "match_all": {}
    }
}

result_arendator = client.search(index=indexName1, body=searchBody)['hits']['hits']
result_arenda = client.search(index=indexName1, body=searchBody)['hits']['hits']
result_car = client.search(index=indexName2, body=searchBody)['hits']['hits']

Schema_for_arendator = StructType([
    StructField("arendator_id", StringType(), True),
    StructField("arendator_data", StringType(), True),
    StructField("arenda_id", StringType(), True)
])

Schema_for_arenda = StructType([
    StructField("arenda_id", StringType(), True),
    StructField("date_of_arenda", StringType(), True),
    StructField("numb_days_of_arend", IntegerType(), True),
    StructField("price", IntegerType(), True),
    StructField("car_id", StringType(), True)
])

Schema_for_car = StructType([
    StructField("car_id", StringType(), True),
    StructField("car_data", StringType(), True),
    StructField("diagnostic_card", StringType(), True),
    StructField("car_reviews", StringType(), True)
])

# arendator table
Table_for_arendator_data = []
for arendator in result_arendator:
    Table_for_arendator_data.append((
        arendator['_source']['arendator_id'],
        arendator['_source']['arendator_data'],
        arendator['_source']['arenda_id']
    ))

# arenda table
Table_for_arenda_data = []
for arenda in result_arenda:
    Table_for_arenda_data.append((
        arenda['_source']['arenda_id'],
        arenda['_source']['date_of_arenda'],
        int(arenda['_source']['numb_days_of_arend']),
        int(arenda['_source']['price']),
        arenda['_source']['car_id']
    ))

# car table
Table_for_car_data = []
for car in result_car:
    Table_for_car_data.append((
        car['_id'],
        car['_source']['car_data'],
        car['_source']['diagnostic_card'],
        car['_source']['car_reviews']
    ))

# Creating data frame
Table_for_arendator = sparkSession.createDataFrame(Table_for_arendator_data, Schema_for_arendator)
Table_for_arenda = sparkSession.createDataFrame(Table_for_arenda_data, Schema_for_arenda)
Table_for_car = sparkSession.createDataFrame(Table_for_car_data, Schema_for_car)

# making csv
Table_for_arendator.write.csv(path='hdfs://localhost:9000/arendator.csv', mode='overwrite', header=True)
Table_for_arenda.write.csv(path='hdfs://localhost:9000/arenda.csv', mode='overwrite', header=True)
Table_for_car.write.csv(path='hdfs://localhost:9000/car.csv', mode='overwrite', header=True)

df_load = sparkSession.read.load(path='hdfs://localhost:9000/arendator.csv', format='csv', sep=',', inferSchema="true", header="true")
df_load.show()
df_load = sparkSession.read.load(path='hdfs://localhost:9000/arenda.csv', format='csv', sep=',', inferSchema="true", header="true")
df_load.show()
df_load = sparkSession.read.load(path='hdfs://localhost:9000/car.csv', format='csv', sep=',', inferSchema="true", header="true")
df_load.show()