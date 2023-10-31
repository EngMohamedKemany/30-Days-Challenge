#!/usr/bin/env python
# coding: utf-8

# CLASSIC APPROACH:


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("proj").getOrCreate()
df = spark.read.text("./02_input.txt")
header = df.take(1)[0][0]
schema = header.split("@|#")
df_rdd = df.filter(~df.value.isin(header)).rdd
df_rdd = df_rdd.map(lambda x: x[0].replace('"', ""))
df = df_rdd.map(lambda x: x.split("@|#")).toDF(schema)
df.show()


# ONE LINER OPTION A:

df = spark.read.option("sep", "@|#").option("header","true").csv("./02_input.txt")
df.show()


# ONE LINER OPTION B:

df = spark.read.option("delimiter", "@|#").option("header","true").csv("./02_input.txt")
df.show()