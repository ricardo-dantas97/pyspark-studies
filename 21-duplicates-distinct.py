# Databricks notebook source
# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df = create_standard_df(type_df='dup')

# COMMAND ----------

display(df)

# COMMAND ----------

# Distinct function to remove duplicates based on all rows
distinct_df = df.distinct()
display(distinct_df)

# COMMAND ----------

# drop duplicates function, we use it to drop based on specific columns
# Drop values based on the name
# We can pass multiple columns in a list

drop_dup_df = df.dropDuplicates(['name'])
display(drop_dup_df)