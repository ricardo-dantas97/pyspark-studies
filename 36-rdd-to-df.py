# Databricks notebook source
# Creating a RDD
data = [(1, 'Ricardo'), (2, 'Jordana')]
rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())

# COMMAND ----------

# Converting RDD to DF, first way
cols = ['id', 'name']  # list to specify the column names
df = rdd.toDF(cols)
display(df)

# COMMAND ----------

# Second way
df = spark.createDataFrame(rdd, cols)
display(df)