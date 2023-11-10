# Databricks notebook source
# MAGIC %md
# MAGIC Partitioning the data while saving it allows us to divide the data based on one or multiple columns making it easier to read later

# COMMAND ----------

# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df = create_standard_df()
df.show()

# COMMAND ----------

# Replacing nulls before saving the data
df = df.fillna('unknown', 'gender')
df.show()

# COMMAND ----------

# Droping null values
display(df.na.drop('any'))

# COMMAND ----------

# Saving df as parquet partitioning by gender

cols_to_partition = ['gender', 'department_id']
path = '/FileStore/data/employees'
df.write.parquet(path, mode='overwrite', partitionBy='gender')  # We could also pass a list of columns to partitionBy argument

# COMMAND ----------

# Reading data
df = spark.read.parquet(path)
df.show()

# COMMAND ----------

# Reading data specifying a partition
# It doesn't return the column that was partitioned, in this case, gender

male_path = path + '/gender=m'
df_male = spark.read.parquet(male_path)
df_male.show()