# Databricks notebook source
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

# Writing df to json
# Spark will create a folder and not a file with the name we specify
# It's important to specify a mode to append, overwrite, etc, to not get an error

df.write.json(path='/FileStore/data/json/', mode='ignore')

# COMMAND ----------

path_delete = ''
dbutils.fs.rm(path_delete, True)