# Databricks notebook source
# MAGIC %md
# MAGIC Function used to convert json string into MapType or ArrayType

# COMMAND ----------

# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

data = [('Ricardo', '{"hair": "brown", "eye": "green", "age": "26"}')]

schema = ['name', 'properties']

df = spark.createDataFrame(data, schema)
df.printSchema()
display(df)

# COMMAND ----------

from pyspark.sql.functions import from_json
from pyspark.sql.types import StringType, IntegerType, StructField, MapType, StructType

# COMMAND ----------

# MapType

# Creating schema to map json column
schema_map = MapType(StringType(), StringType())

df = df.withColumn('properties_map', from_json(df.properties, schema_map))
display(df)

# COMMAND ----------

# StructType

# Creating schema to map json column
schema_struct = StructType(
  [StructField('hair', StringType()),
   StructField('eye', StringType()),
   StructField('age', StringType())]
)

df = df.withColumn('properties_struct', from_json(df.properties, schema_struct))
display(df)

# COMMAND ----------

# Accesing values from these new columns
df = df.withColumn('hair', df.properties_struct.hair) \
       .withColumn('eye', df.properties_map.eye) \
       .withColumn('age', df.properties_struct.age)

display(df.select('name', 'hair', 'eye', 'age'))