# Databricks notebook source
# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df = create_standard_df()

# COMMAND ----------

# Selecting all columns
display(
  df.select('*')
)

# COMMAND ----------

# Selecting specific columns
cols = ['name', 'salary']
display(df.select(cols))