# Databricks notebook source
# MAGIC %md
# MAGIC Using transform function from sql.functions  
# MAGIC Only used with array type columns

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import transform, upper

# COMMAND ----------

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

# Using lambda function

display(
  df.select(
    'name',
    transform( 'skills', lambda x: upper(x)).alias('skills') )
)

# COMMAND ----------

# Using a defined function
def convert_list_elements_to_upper(value):
  return upper(value)

display(
  df.select(
    'name',
    transform('skills', convert_list_elements_to_upper).alias('skills')
  )
)