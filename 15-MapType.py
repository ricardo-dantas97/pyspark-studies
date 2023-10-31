# Databricks notebook source
# MAGIC %md
# MAGIC MapType column is like a dict in Python

# COMMAND ----------

from pyspark.sql.types import MapType, StringType

data = [
  ('Ricardo', {'number': 10, 'age': 26}),
  ('Jordana', {'number': 13, 'age': 25}),
]

schema = StructType().add('name', StringType()) \
                     .add('attributes', MapType(StringType(), IntegerType()))

df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

# Acessing keys from map columns
display(
  df.withColumn('number', df['attributes']['number']) \
    .withColumn('age', df['attributes']['age'])
)