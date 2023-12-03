rom elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime

client = Elasticsearch([{"host": "127.0.0.1", "port": 9200}])
indexName1 = "salon_procedure"
indexName2 = "salon_master"

sparkSession = SparkSession.builder.appName("spark-csv").getOrCreate()
searchBody = {
    "size": 60,
    "query": {
        "match_all": {}
    }
}

result_procedure = client.search(index=indexName1, body=searchBody)['hits']['hits']
result_master = client.search(index=indexName2, body=searchBody)['hits']['hits']
result_client = client.search(index=indexName1, body=searchBody)['hits']['hits']

# schema for procedure, master and client
Schema_for_procedure = StructType([
    StructField("procedure_id", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("procedure_date", DateType(), True),
    StructField("master_id", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("service", StringType(), True),
    StructField("cosmetics", StringType(), True)
])

Schema_for_master = StructType([
    StructField("master_id", StringType(), True),
    StructField("master_specialization", StringType(), True),
    StructField("master_experience", IntegerType(), True),
    StructField("master_personal_data", StringType(), True),
    StructField("schedule", StringType(), True),
    StructField("rewiews", StringType(), True)
])

Schema_for_client = StructType([
    StructField("client_id", StringType(), True),
    StructField("client_personal_data", StringType(), True),
    StructField("client_age", IntegerType(), True)])
# procedure table
Table_for_procedure_data = []
for procedure in result_procedure:
    Table_for_procedure_data.append((
        procedure['_source']['id_of_procedure'],
        procedure['_source']['id_of_client'],
        datetime.strptime(procedure['_source']['date_of_procedure'], "%Y-%m-%d"),
        procedure['_source']['id_of_specialist'],
        int(procedure['_source']['price']),
        procedure['_source']['service'],
        procedure['_source']['cosmetics']
    ))

# master table
Table_for_master_data = []
for master in result_master:
    Table_for_master_data.append((
        master['_id'],
        master['_source']['specialisation'],
        int(master['_source']['work_experience']),
        master['_source']['master_personal_data'],
        master['_source']['schedule'],
        master['_source']['reviews']
    ))


# client table
Table_for_client_data = []
for client in result_client:
    Table_for_client_data.append((
        client['_source']['id_of_client'],
        client['_source']['client_personal_data'],
        int(client['_source']['client_age'])
    ))

#Creating data frame
Table_for_procedure = sparkSession.createDataFrame(Table_for_procedure_data, Schema_for_procedure)
Table_for_master = sparkSession.createDataFrame(Table_for_master_data, Schema_for_master)
Table_for_client = sparkSession.createDataFrame(Table_for_client_data, Schema_for_client)

# making csv
Table_for_procedure.write.csv(path='hdfs://localhost:9000/procedure.csv', mode='overwrite', header=True)
Table_for_master.write.csv(path='hdfs://localhost:9000/master.csv', mode='overwrite', header=True)
Table_for_client.write.csv(path='hdfs://localhost:9000/client.csv', mode='overwrite', header=True)

df_load = sparkSession.read.load(path='hdfs://localhost:9000/procedure.csv', format='csv', sep=',', inferSchema="true", header="true")
df_load.show()
df_load = sparkSession.read.load(path='hdfs://localhost:9000/master.csv', format='csv', sep=',', inferSchema="true", header="true")
df_load.show()
df_load = sparkSession.read.load(path='hdfs://localhost:9000/client.csv', format='csv', sep=',', inferSchema="true", header="true")
df_load.show()