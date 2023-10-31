# Databricks notebook source
# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df = create_standard_df()
display(df)

# COMMAND ----------

# Renaming a column
df = df.withColumnRenamed(existing='name', new='person_name')
display(df)