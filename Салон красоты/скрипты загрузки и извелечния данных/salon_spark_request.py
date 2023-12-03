#!/usr/bin/env python
# -*- coding: utf-8 -*-from pyspark.sql import SparkSession

sparkSession=SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "5").getOrCreate()

Procedure_Table = sparkSession.read.load(path='hdfs://localhost:9000/procedure.csv', format='csv', sep=',', inferSchema="true", header="true")
Master_Table = sparkSession.read.load(path='hdfs://localhost:9000/master.csv', format='csv', sep=',', inferSchema="true", header="true")
Client_Table = sparkSession.read.load(path='hdfs://localhost:9000/client.csv', format='csv', sep=',', inferSchema="true", header="true")

Procedure_Table.registerTempTable("procedure")
Master_Table.registerTempTable("master")
Client_Table.registerTempTable("client")

df = sparkSession.sql("SELECT procedure.master_id, COUNT(procedure.id) FROM procedure GROUP BY procedure.master_id").show()

input('Ctrl C')