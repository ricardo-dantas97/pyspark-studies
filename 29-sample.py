# Databricks notebook source
# MAGIC %md
# MAGIC Sample function returns random rows from a df

# COMMAND ----------

# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df = create_standard_df()

# COMMAND ----------

# The quantity of rows is not constant, sometimes it returns more, sometimes less. Fraction 0.1 means 10%
df_sample = df.sample(fraction=0.1)
display(df_sample)

# COMMAND ----------

# Seed parameter guarantee the same df if we need
df_sample = df.sample(fraction=0.1, seed=5)
df_sample2 = df.sample(fraction=0.1, seed=5)

display(df_sample)
display(df_sample2)