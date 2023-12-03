#!/usr/bin/env python
# -*- coding: utf-8 -*-from pyspark.sql import SparkSession

sparkSession=SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "5").getOrCreate()

Arendator_Table = sparkSession.read.load(path='hdfs://localhost:9000/arendator.csv', format='csv', sep=',', inferSchema="true", header="true")
Arenda_Table = sparkSession.read.load(path='hdfs://localhost:9000/arenda.csv', format='csv', sep=',', inferSchema="true", header="true")
Car_Table = sparkSession.read.load(path='hdfs://localhost:9000/car.csv', format='csv', sep=',', inferSchema="true", header="true")

Arendator_Table.registerTempTable("arendator")
Arenda_Table.registerTempTable("arenda")
Car_Table.registerTempTable("car")

df = sparkSession.sql("SELECT arendator.arendator_id, arendator.arendator_data, car.car_id, car.car_data, arenda.price FROM arendator, arenda, car WHERE (arenda.price = (SELECT MAX(price) FROM arenda)) and (arendator.arenda_id = arenda.arenda_id) and (arenda.car_id = car.car_id)").show()

input('Ctrl C')