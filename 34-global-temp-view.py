# Databricks notebook source
# MAGIC %md
# MAGIC We can create views from a df and work with SQL  
# MAGIC Global temp views can be acessed across the sessions

# COMMAND ----------

# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df = create_standard_df()

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // Getting spark session ID
# MAGIC spark

# COMMAND ----------

# Creating temp view
df.createOrReplaceGlobalTempView('vw_employees_global')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Using SQL
# MAGIC SELECT *
# MAGIC FROM global_temp.vw_employees_global
# MAGIC LIMIT 5

# COMMAND ----------

# Creating df from view

query = """
  SELECT *
  FROM global_temp.vw_employees_global
  WHERE gender = 'f'
"""

df_female = spark.sql(query)

# COMMAND ----------

display(df_female)

# COMMAND ----------

# Validating current DB
current_df = spark.catalog.currentDatabase()
current_df

# COMMAND ----------

# Validating tables
spark.catalog.listTables(current_df)

# COMMAND ----------

# Validating tables on global temp
spark.catalog.listTables('global_temp')

# COMMAND ----------

# Drop temp view
spark.catalog.dropGlobalTempView('vw_employees')