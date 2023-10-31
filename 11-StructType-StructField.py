# Databricks notebook source
# MAGIC %md
# MAGIC StructType and StructField are objects used when we want to create a schema for our df  
# MAGIC StructType is a collection of StructFields  
# MAGIC They need to be imported from pyspark.sql.types

# COMMAND ----------

from pyspark.sql.types import *

data = [
  (1, 'Ricardo', 5000.0),
  (2, 'Jordana', 8000.0),
]

# Simple schema
schema = StructType(
  [ StructField(name='id', dataType=IntegerType()),
    StructField(name='name', dataType=StringType()),
    StructField(name='salary', dataType=DoubleType()) ]
)

df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

# Creating a complex schema
data = [
  (1, ('Ricardo', 'Dantas'), 5000.0),
  (2, ('Jordana', 'Calegari'), 8000.0),
]

# Variable to hold name and last name
name = StructType(
  [StructField('name', dataType=StringType()),
   StructField('last_name', dataType=StringType())]
)

schema = StructType(
  [ StructField(name='id', dataType=IntegerType()),
    StructField(name='name', dataType=name),
    StructField(name='salary', dataType=DoubleType()) ]
)

df = spark.createDataFrame(data, schema)
display(df)
df.printSchema()