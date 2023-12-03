#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, expr, concat, col

sparkSession=SparkSession.builder.appName("spark-csv").config("spark.some.config.option", "5").getOrCreate()

Procedure_Table = sparkSession.read.load(path='hdfs://localhost:9000/procedure.csv', format='csv', sep=',', inferSchema="true", header="true")
Patient_Table = sparkSession.read.load(path='hdfs://localhost:9000/patient.csv', format='csv', sep=',', inferSchema="true", header="true")
Naznachenie_Table = sparkSession.read.load(path='hdfs://localhost:9000/naznachenie.csv', format='csv', sep=',', inferSchema="true", header="true")

Procedure_Table.registerTempTable("procedure")
Patient_Table.registerTempTable("patient")
Naznachenie_Table.registerTempTable("naznachenie")

df = sparkSession.sql("SELECT ID_OF_PROCEDURE, COUNT(ID_OF_PROCEDURE) as NUMBER_OF_NAZNACH FROM (SELECT procedure.procedure_id as ID_OF_PROCEDURE, naznachenie.procedures_id LIKE concat(concat('%', procedure.procedure_id),'%') as CHECK, naznachenie.procedures_id FROM procedure, naznachenie) WHERE check=true GROUP BY ID_OF_PROCEDURE ORDER BY ID_OF_PROCEDURE").show(40)

input('Ctrl C')