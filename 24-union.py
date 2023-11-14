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

# MAGIC %md
# MAGIC Union by name function allows us to union two dfs even if they don't have the same schema  
# MAGIC Columns that don't fit, get null values

# COMMAND ----------

# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df1 = create_standard_df()
df2 = create_standard_df()

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

from pyspark.sql.functions import when

df1 = df1.withColumn(
  'status',
  when(df1['salary'] < 5000, 'Bad') \
  .when(df1['salary'] < 10000, 'OK') \
  .otherwise('Great')
)

display(df1.sort('salary'))

# COMMAND ----------

# Union dfs with different schema

new_df = df1.unionByName(allowMissingColumns=True, other=df2)
display(new_df.sort('salary'))

# COMMAND ----------

# Replacing null values

display(
  new_df.fillna('N/A', subset=['status'])
)