# Databricks notebook source
# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df = create_standard_df()

# COMMAND ----------

from pyspark.sql.functions import when

# Creating conditional column with case when
display(
  df.select(
    df.name,
    when(df.name == 'Ricardo', 'Male') \
      .when(df.name == 'Jordana', 'Female')
      .otherwise('Unknown').alias('gender'),
    when(df.salary > 10000, 'Good').otherwise('Bad').alias('status')
  )
)