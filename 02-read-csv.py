# Databricks notebook source
# First way of reading a CSV file
df = spark.read.csv(
  path='dbfs:/FileStore/tables/electric_chargepoints_2017.csv',
  header=True
)

# COMMAND ----------

# Another way
df = spark.read.format('csv') \
          .option('header', True) \
          .load(path='dbfs:/FileStore/tables/electric_chargepoints_2017.csv')

# COMMAND ----------

# Create df using more than one file
# On path parameter, we pass a list with path files.
# They need to have the same schema

files = [
  'dbfs:/FileStore/tables/electric_chargepoints_2017.csv',
  'dbfs:/FileStore/tables/electric_chargepoints_2017-1.csv'
]

df = spark.read.format('csv') \
          .option('header', True) \
          .load(path=files)

# COMMAND ----------

# Creating a df with all CSV files in a folder
# We need to specify the path of the folder

df = spark.read.format('csv') \
          .option('header', True) \
          .load(path='dbfs:/FileStore/csv/')

# COMMAND ----------

# Defining a schema for df

from pyspark.sql.types import *

schema = StructType().add(field='ChargingEvent', data_type=StringType()) \
                     .add(field='CPID', data_type=StringType()) \
                     .add(field='StartDate', data_type=DateType()) \
                     .add(field='StartTime', data_type=StringType()) \
                     .add(field='EndDate', data_type=DateType()) \
                     .add(field='EndTime', data_type=StringType()) \
                     .add(field='Energy', data_type=DoubleType()) \
                     .add(field='PluginDuration', data_type=DoubleType())

path = 'dbfs:/FileStore/tables/electric_chargepoints_2017.csv'

df = spark.read.format('csv') \
          .option('header', True) \
          .schema(schema) \
          .load(path=path)

# df = spark.read.csv(
#   path='dbfs:/FileStore/tables/electric_chargepoints_2017.csv',
#   header=True,
#   schema=schema
# )

# COMMAND ----------

from pyspark.sql.functions import *

grouped_df = df.groupBy('CPID') \
               .agg(
                 round(mean('PluginDuration'), 2).alias('mean_duration'),
                 round(sum('PluginDuration'), 2).alias('sum_duration')
              )
               
grouped_df = grouped_df.withColumnRenamed('CPID', 'event_id')

display(grouped_df)

# COMMAND ----------

count_df = grouped_df.groupby('event_id') \
                     .agg(count('event_id').alias('qtd'))

# COMMAND ----------

display(count_df)