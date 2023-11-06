import pyspark
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("proj").getOrCreate()

df = spark.read.option("header","true").csv("./03_input.csv")
df = df.withColumn("Date", to_date(col("RechargeDate"),"yyyyMMdd"))
#OPTION A
df = df.withColumn("NextDate", date_add("Date", F.col("RemainingDays").cast("int")))
df.show()

#OPTION B
query = "date_add(Date, CAST(RemainingDays AS INT)) AS EndDate"
df.select(expr(query)).show()