# Databricks notebook source
from pyspark.sql import *

# COMMAND ----------

data = [
  {'name':'Ricardo', 'surname':'Dantas'},
  {'name':'Jordana', 'surname':'Calegari'}
]

df = spark.createDataFrame(data)

# COMMAND ----------

display(df)

# COMMAND ----------

# First way
df.write.mode('overwrite').option('header', True).csv('/tmp/csv')

# COMMAND ----------

# Another way
df.write.options(header=True).mode('append').csv('/tmp/csv')

# COMMAND ----------

df = spark.read.csv(path='/tmp/csv', header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

# Excluindo arquivos
dbutils.fs.rm("/tmp/csv", True)