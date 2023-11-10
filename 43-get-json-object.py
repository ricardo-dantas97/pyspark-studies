# Databricks notebook source
# MAGIC %md
# MAGIC Function that creates new columns from json string

# COMMAND ----------

from pyspark.sql.functions import get_json_object, json_tuple

data = [('Ricardo', '{"address": {"city": "Barueri", "state": "Sao Paulo"}, "gender": "male"}')]

schema = ['name', 'properties']

df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

# We can pass many keys from the json to the function

display(
  df.select(
    df.name,
    get_json_object(df.properties, '$.gender').alias('gender'),
    get_json_object(df.properties, '$.address.city').alias('city'),
    json_tuple(df.properties, '$.address.state').alias('state')
  )
)