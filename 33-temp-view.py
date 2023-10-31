# Databricks notebook source
# MAGIC %md
# MAGIC We can create views from a df and work with SQL  
# MAGIC Temp views are only available in the current session

# COMMAND ----------

# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df = create_standard_df()

# COMMAND ----------

# Creating temp view
df.createOrReplaceTempView('vw_employees')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Using SQL
# MAGIC SELECT *
# MAGIC FROM vw_employees
# MAGIC LIMIT 7

# COMMAND ----------

# Creating df from view

query = """
  SELECT *
  FROM vw_employees
  WHERE gender = 'f'
"""

df_female = spark.sql(query)

# COMMAND ----------

display(df_female)