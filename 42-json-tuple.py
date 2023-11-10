# Databricks notebook source
# MAGIC %md
# MAGIC Function that creates new columns from json string

# COMMAND ----------

from pyspark.sql.functions import json_tuple

data = [('Ricardo', '{"hair": "brown", "eye": "green", "age": "26"}')]

schema = ['name', 'properties']

df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

# We can pass many keys from the json to the function

display(
  df.select(
    df.name,
    json_tuple(df.properties, 'hair', 'eye', 'age').alias('hair', 'eye', 'age')
  )
)