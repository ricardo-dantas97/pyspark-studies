# Databricks notebook source
# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

from pyspark.sql.functions import when

df = create_standard_df()
df_dep = create_departments_df()

df = df.withColumn('gender', when(df['gender'] == 'm', 'Male').otherwise('Female'))

df = df.join(
  df_dep,
  df_dep['id'] == df['department_id'],
  'left'
).select(
  df['name'],
  df['gender'],
  df_dep['name'].alias('department_name')
)

display(df)

# COMMAND ----------

display(
  df.groupBy('gender', 'department_name').count()
)

# COMMAND ----------

# Pivoting by gender column
# All distinct values from gender column will become new columns
# We can pass a list of values if we don't want every value to become a new column

df = df.groupBy('department_name').pivot('gender', ['Male', 'Female']).count()

df = df.withColumnRenamed('Male', 'male') \
       .withColumnRenamed('Female', 'female')

display(df)

# COMMAND ----------

# Unpivoting column

from pyspark.sql.functions import expr

display(
  df.select(
    df.department_name,
    expr("stack(2, 'male', male, 'female', female) as (gender, count)")
  )
)