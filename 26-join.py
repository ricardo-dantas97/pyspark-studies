# Databricks notebook source
# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df = create_standard_df()
departments_df = create_departments_df()

# COMMAND ----------

# Inner join and select specific columns

display(
  df.join(
    departments_df,
    df['department_id'] == departments_df['id'],
    how='inner'
  ).select(df['name'], df['salary'], departments_df['name'].alias('department'))
)

# COMMAND ----------

# Left join

display(
  df.join(
    departments_df,
    df['department_id'] == departments_df['id'],
    how='left'
  ).select(df['name'], df['salary'], departments_df['name'].alias('department'))
)

# COMMAND ----------

# Right join

display(
  df.join(
    departments_df,
    df['department_id'] == departments_df['id'],
    how='right'
  ).select(df['name'], df['salary'], departments_df['name'].alias('department'))
)

# COMMAND ----------

# Leftsemi join
# Acts like a inner join but only returns the columns from the left df

display(
  df.join(
    departments_df,
    df['department_id'] == departments_df['id'],
    how='leftsemi'
  )
)

# COMMAND ----------

# Leftanti join
# Acts like a left join but only returns the columns from the left df that don't match with the right one

display(
  df.join(
    departments_df,
    df['department_id'] == departments_df['id'],
    how='leftanti'
  )
)

# COMMAND ----------

# Self join
# Join with the same df
# So far, I understood that we need to give an alias to df and use col function in the condition

from pyspark.sql.functions import col

display(
  df.alias('emp_df').join(
    df.alias('manager_df'),
    col('emp_df.manager_id') == col('manager_df.id'),
    how='left'
  ).select(
    col('emp_df.id'),
    col('emp_df.name').alias('emp_name'),
    col('manager_df.name').alias('manager_name')
  ).orderBy(col('emp_df.id'))
)