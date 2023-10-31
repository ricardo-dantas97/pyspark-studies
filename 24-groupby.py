# Databricks notebook source
# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df = create_standard_df()
df2 = create_standard_df(type_df='dup')

# COMMAND ----------

# Union function will append one df to another, increasing the quantity of lines
display(df.union(df2))

# COMMAND ----------

# If we want to remove duplicates, we can use distinct function together
new_df = df.union(df2).distinct()
display(new_df.sort(df['id']))