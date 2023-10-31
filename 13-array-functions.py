# Databricks notebook source
# MAGIC %md
# MAGIC Array functions

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import explode, split, array, array_contains, lit

data = [
  ('Ricardo', ['aws', 'sql', 'python']),
  ('Jordana', ['python', 'azure']),
]

schema = StructType(
  [ StructField(name='name', dataType=StringType()),
    StructField(name='skills', dataType=ArrayType(elementType=StringType())) ]
)

df = spark.createDataFrame(data, schema)
display(df)
df.printSchema()

# COMMAND ----------

# Explode function. Create one row for each element of the array
df_explode = df.withColumn('skill', explode(df.skills))
display(df_explode)

# COMMAND ----------

# Split function. Return an array from a string by some delimiter
data = [
  ('Ricardo', 'aws, sql, python'),
  ('Jordana', 'python, azure'),
]

schema = StructType(
  [ StructField(name='name', dataType=StringType()),
    StructField(name='skills', dataType=StringType()) ]
)

df_split = spark.createDataFrame(data, schema)
display(df_split)
df_split = df_split.withColumn('skills_list', split(df_split.skills, ','))
display(df_split)

# COMMAND ----------

# array function. Create a new array column by merging data from multiple columns
data = [
  ('Ricardo', 'Dantas', 26),
  ('Jordana', 'Calegari', 25),
]

schema = StructType(
  [ StructField(name='name', dataType=StringType()),
    StructField(name='last_name', dataType=StringType()), 
    StructField(name='age', dataType=IntegerType()) ]
)

array_df = spark.createDataFrame(data, schema)
display(array_df)

array_df = array_df.withColumn('names', array(array_df.name, array_df.last_name))
display(array_df)

# COMMAND ----------

# array_contains function. Check if a value exists in an array
# It's case sensitive, so the string must be exactly the same

df_contains = array_df.withColumn(
  'col_validation',
  array_contains(array_df.names, 'Dantas')
)

display(df_contains)