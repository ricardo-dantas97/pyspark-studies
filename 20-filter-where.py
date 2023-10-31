# Databricks notebook source
# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df = create_standard_df()

# COMMAND ----------

display(df)

# COMMAND ----------

# Filter function

display(df.filter((df['salary'] < 10000) | (df['name'].like('R%'))))

# COMMAND ----------

# Where function

condition = (df['salary'] < 10000) & (df['name'].like('R%'))
display(df.where(condition))