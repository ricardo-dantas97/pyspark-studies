# Databricks notebook source
from pyspark.sql.functions import current_timestamp, to_timestamp, lit, hour, minute, second

# COMMAND ----------

df = spark.range(2)

# COMMAND ----------

# Adding column with current date
df = df.withColumn('cur_timestamp', current_timestamp())
display(df)

# COMMAND ----------

# Converting a string to a timestamp
df = df.withColumn('string_timestamp', to_timestamp(lit('03-09-1997 10.10.10'), 'dd-MM-yyyy HH.mm.ss'))
display(df)

# COMMAND ----------

# Getting hour, minute and seconds from timestamp
display(
  df.select(
    '*',
    hour(df.cur_timestamp).alias('hour'),
    minute(df.cur_timestamp).alias('minute'),
    second(df.cur_timestamp).alias('second'),
  )
)