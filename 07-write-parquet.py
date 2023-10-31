# Databricks notebook source
# Writing parquet file

from pyspark.sql.types import *

# Creating df
data = [
  {'id': 1, 'name': 'Ricardo'},
  {'id': 2, 'name': 'Jordana'},
]

schema = StructType().add(field='id', data_type=IntegerType()) \
                     .add(field='name', data_type=StringType())

df = spark.createDataFrame(data, schema=schema)

# COMMAND ----------

# Saving df as parquet
path = '/FileStore/data/parquet/'
df.write.parquet(path, mode='overwrite')