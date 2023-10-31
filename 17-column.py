# Databricks notebook source
# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df = create_standard_df()

# COMMAND ----------

# Creating new column with some string

from pyspark.sql.functions import lit
display(df.withColumn('source', lit('PROD')))

# COMMAND ----------

# Select function is like SQL to specify columns to return

from pyspark.sql.functions import col

display(df.select(df.id, df['name'], col('salary')))