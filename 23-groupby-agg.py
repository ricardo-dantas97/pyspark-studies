# Databricks notebook source
# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df = create_standard_df()

# COMMAND ----------

from pyspark.sql.functions import *

df = df.withColumn('gender', when(df['gender'] == 'm', 'Male').otherwise('Female'))

# COMMAND ----------

# Only one aggregation

display(
  df.groupBy('gender').max('salary')
)

# COMMAND ----------

# Using agg to multiple aggregations

display(
  df.groupBy('gender').agg(
    min('salary').alias('min_salary'),
    max('salary').alias('max_salary'),
    mean('salary').alias('mean_salary'),
    count('salary').alias('qtd')
  )
)