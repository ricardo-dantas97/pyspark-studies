# Databricks notebook source
# MAGIC %md
# MAGIC Function that converts an python dict into a JSON string

# COMMAND ----------

from pyspark.sql.functions import to_json

data = [('Ricardo', {"hair": "brown", "eye": "green", "age": "26"})]

schema = ['name', 'properties']

df = spark.createDataFrame(data, schema)
df.printSchema()
display(df)

# COMMAND ----------

# Creating new json column
df = df.withColumn('properties_json', to_json(df.properties))

# COMMAND ----------

display(df)
df.printSchema()