# Databricks notebook source
# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df = create_df()

# COMMAND ----------

display(df)

# COMMAND ----------

# Show command presents the df. 
# It truncates columns and show only the first twenty characters
# It shows only 20 rows also, but we can pass another value as a parameter to see more rows

df.show(
  truncate=False,
  n=50
)