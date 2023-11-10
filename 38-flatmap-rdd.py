# Databricks notebook source
# MAGIC %md
# MAGIC flatMap transformation is used to flatten RDD array columns after applying the function on every element

# COMMAND ----------

# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

data = ['Ricardo Dantas', 'Jordana Ribeiro']
rdd = create_basic_rdd(data)

# COMMAND ----------

# Using map function to create a list of values with split function
rdd1 = rdd.map(lambda x: x.split(' '))

for v in rdd1.collect():
  print(v)

# COMMAND ----------

# Using flatMap to flatten the values
rdd2 = rdd.flatMap(lambda x: x.split(' '))

for v in rdd2.collect():
  print(v)