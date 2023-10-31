# Databricks notebook source
# MAGIC %md
# MAGIC ArrayType columns

# COMMAND ----------

from pyspark.sql.types import *

data = [
  ('Ricardo', [10, 8]),
  ('Jordana', [1, 13]),
]

schema = StructType(
  [ StructField(name='name', dataType=StringType()),
    StructField(name='numbers', dataType=ArrayType(elementType=IntegerType())) ]
)

df = spark.createDataFrame(data, schema)
display(df)
df.printSchema()

# COMMAND ----------

# Creating new column using values from the array
df = df.withColumn('first_number', df.numbers[0])
display(df)