# Databricks notebook source
# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df = create_standard_df()
display(df)

# COMMAND ----------

from pyspark.sql.functions import approx_count_distinct, avg, collect_list, collect_set, countDistinct, count

# Count of distinct salaries
display(df.select(approx_count_distinct('salary').alias('qtd_distinct')))
display(df.select(avg('salary').alias('avg_salary')))
display(df.select(count('salary').alias('salary_count')))
display(df.select(countDistinct('salary').alias('salary_distinct_count')))

# COMMAND ----------

# Getting a list of values from a column
display(df.select(collect_list(df.salary)))  # this returns duplicates
display(df.select(collect_set(df.salary)))   # this returns only distinct values