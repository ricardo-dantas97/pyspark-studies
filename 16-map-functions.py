# Databricks notebook source
# MAGIC %md
# MAGIC MapType column is like a dict in Python

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import explode, map_keys, map_values

data = [
  ('Ricardo', {'number': 10, 'age': 26}),
  ('Jordana', {'number': 13, 'age': 25}),
]

schema = StructType().add('name', StringType()) \
                     .add('attributes', MapType(StringType(), IntegerType()))

df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

# Explode function create new rows and columns from the key value column
# One row for each key and value
display(df.select('name', 'attributes', explode(df['attributes'])))

# COMMAND ----------

# map_keys function will create a new column with keys from an array
display(
  df.withColumn('keys', map_keys(df.attributes))
)

# COMMAND ----------

# map_values function will create a new column with values from an array
display(
  df.withColumn('values', map_values(df.attributes))
)