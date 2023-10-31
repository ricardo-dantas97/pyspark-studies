# Databricks notebook source
# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df = create_standard_df()

# COMMAND ----------

# Alias function to show a specific value in a column name

display(
  df.select('*', df['name'].alias('person_name'))
)

# COMMAND ----------

# Sorting data
display(
  df.select('*').sort(df['salary'], df['name'].asc())
)

display(
  df.select('*').sort(df['name'].desc())
)

# COMMAND ----------

# Converting data type using cast function
df.printSchema()

df = df.select(
    df.id,
    df.name,
    df.salary.cast('int')
  )

df.printSchema()

# COMMAND ----------

# Like function to filter
from pyspark.sql.functions import lower, upper

display(
  df.filter(df.name.like('R%')).select('*')
)

display(
  df.select('*').filter(df.name.like('R%'))
)

display(
  df.select('*').filter(lower(df.name)== 'ricardo')
)