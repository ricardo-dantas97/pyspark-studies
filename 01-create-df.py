# Databricks notebook source
# Importing pyspark types
from pyspark.sql.types import *

# COMMAND ----------

# Creating data for df
data = [
  (1, 'Ricardo'),
  (2, 'Jordana')
]

# We can also use a dictionary for data
data = [
  {'id':1, 'name':'Ricardo'},
  {'id':2, 'name':'Jordana'}
]

# Creating schema for df
schema = StructType(
  [
    StructField(name='id', dataType=IntegerType()),
    StructField(name='name', dataType=StringType())
  ]
)

df = spark.createDataFrame(data=data, schema=schema)

# COMMAND ----------

display(df)

# COMMAND ----------

# Validating df schema
df.printSchema()