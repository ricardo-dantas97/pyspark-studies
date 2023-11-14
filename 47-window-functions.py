# Databricks notebook source
# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

from pyspark.sql.functions import row_number, rank, dense_rank
from pyspark.sql.window import Window

person_df = create_standard_df()
df_dep = create_departments_df()
display(df)
display(df_dep)

# COMMAND ----------

df = person_df.join(
  df_dep, 
  df_dep.id == person_df.department_id
)

df = df.select(person_df.name, person_df.salary, person_df.gender, df_dep.name.alias('department'))

# COMMAND ----------

# Creating a window
window = Window.partitionBy('department').orderBy(df.salary.desc())

df = df.withColumn('row_number', row_number().over(window)) \
        .withColumn('rank', rank().over(window)) \
        .withColumn('dense_rank', dense_rank().over(window))


display(df.orderBy('department'))