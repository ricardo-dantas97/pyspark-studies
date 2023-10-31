# Databricks notebook source
# Reading a simple json file
df = spark.read.json(path='/FileStore/data/data.json')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Reading multiline json file
df_ml = spark.read.option('multiline', True) \
        .json(path='/FileStore/data/multiline.json')

# Another way
df_ml = spark.read.json(path='/FileStore/data/multiline.json', multiLine=True)

# COMMAND ----------

# Reading multiple files, we pass a list with the path
files = ['/FileStore/data/data.json', '/FileStore/data/data2.json']

files_df = spark.read.json(path=files)

# COMMAND ----------

# Reading all files from a folder, we can use a wildcard
folder_df = spark.read.json(path='/FileStore/data/*.json')

# COMMAND ----------

# Reading a json file with a defined schema

from pyspark.sql.types import *

schema = StructType().add(field='id', data_type=IntegerType()) \
                     .add(field='name', data_type=StringType()) \
                     .add(field='salary', data_type=DoubleType())

df = spark.read.json(path=files, schema=schema)

# COMMAND ----------

df.printSchema()