# Databricks notebook source
# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df = create_standard_df()
df.printSchema()
display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Creating new column
calc = col('salary') * 1.05
calc = df.salary * 1.05
df = df.withColumn('new_salary', calc)
display(df)

# COMMAND ----------

# Changing column data type
df = df.withColumn('new_salary', col('new_salary').cast('Integer'))
df.printSchema()
display(df)

# COMMAND ----------

# Updating value of a column
df = df.withColumn('new_salary', col('new_salary') * 2)
display(df)