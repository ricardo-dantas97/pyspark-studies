# Databricks notebook source
# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df = create_standard_df()

# COMMAND ----------

# order by function
display(df.orderBy(df['name'].desc(), df['salary'].desc()))

# COMMAND ----------

# sort function
display(
  df.sort( df['name'].asc(), df['salary'].asc() )
)