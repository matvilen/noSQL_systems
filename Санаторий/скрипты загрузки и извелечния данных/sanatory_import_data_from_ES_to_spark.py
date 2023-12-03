from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime
#import findspark
#findspark.init()

client = Elasticsearch([{"host": "localhost", "port": 9200}])
indexName1 = "procedure"
indexName2 = "patient"

sparkSession = SparkSession.builder.appName("spark-csv").getOrCreate()
searchBody = {
    "size": 60,
    "query": {
        "match_all": {}
    }
}

result_procedure = client.search(index=indexName1, body=searchBody)['hits']['hits']
result_patient = client.search(index=indexName2, body=searchBody)['hits']['hits']
result_naznachenie = client.search(index=indexName2, body=searchBody)['hits']['hits']

# schema for procedure, patient and naznachenie
Schema_for_procedure = StructType([
    StructField("procedure_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("price", IntegerType(), True),
])

Schema_for_patient = StructType([
    StructField("patient_id", StringType(), True),
StructField("personal_data", StringType(), True),
StructField("voucher", StringType(), True),
StructField("diagnosis", StringType(), True)
])

Schema_for_naznachenie= StructType([
    StructField("voucher", StringType(), True), 
    StructField("procedures_id", StringType(), True),
])
# procedure table
Table_for_procedure_data = []
for procedure in result_procedure:
    Table_for_procedure_data.append((
        procedure['_id'],
        procedure['_source']['name'],
        procedure['_source']['description'],
        int(procedure['_source']['price'])
))
   
# paient table
Table_for_patient_data = []
for patient in result_patient:
    Table_for_patient_data.append((
        patient['_source']['patient_id'],
        patient['_source']['personal_data'],
        patient['_source']['voucher'],
        patient['_source']['diagnosis'],
    ))


# naznachenie table
Table_for_naznachenie_data = []
for naznachenie in result_naznachenie:
    Table_for_naznachenie_data.append((
        naznachenie['_source']['voucher'],
        naznachenie['_source']['precedures_id']
    ))

#Creating data frame
Table_for_procedure = sparkSession.createDataFrame(Table_for_procedure_data, Schema_for_procedure)
Table_for_patient = sparkSession.createDataFrame(Table_for_patient_data, Schema_for_patient)
Table_for_naznachenie = sparkSession.createDataFrame(Table_for_naznachenie_data, Schema_for_naznachenie)

# making csv
Table_for_procedure.write.csv(path='hdfs://localhost:9000/procedure.csv', mode='overwrite', header=True)
Table_for_patient.write.csv(path='hdfs://localhost:9000/patient.csv', mode='overwrite', header=True)
Table_for_naznachenie.write.csv(path='hdfs://localhost:9000/naznachenie.csv', mode='overwrite', header=True)

df_load = sparkSession.read.load(path='hdfs://localhost:9000/procedure.csv', format='csv', sep=',', inferSchema="true", header="true")
df_load.show()
df_load = sparkSession.read.load(path='hdfs://localhost:9000/patient.csv', format='csv', sep=',', inferSchema="true", header="true")
df_load.show()
df_load = sparkSession.read.load(path='hdfs://localhost:9000/naznachenie.csv', format='csv', sep=',', inferSchema="true", header="true")
df_load.show()
