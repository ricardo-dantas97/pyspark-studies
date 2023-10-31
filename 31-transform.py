# Databricks notebook source
# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df = create_standard_df()

# COMMAND ----------

# Transform function allows us to create a custom function and use it in our df
from pyspark.sql.functions import upper, round, col

def convert_string_to_upper(df, *args):
  for col in args:
    df = df.withColumn(col, upper(df[col]))
  return df

def raise_salary(df, percentage=10):
  percentage = 1 + (percentage/100)
  df = df.withColumn('new_salary', round(df['salary'] * percentage))
  return df

# First we pass the function name, then, we pass the arguments separeted by comma. We don't use ()
df = df.transform(convert_string_to_upper, 'name', 'gender') \
       .transform(raise_salary, 35)

display(df)