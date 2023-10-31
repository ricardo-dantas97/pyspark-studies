# Databricks notebook source
# Reading parquet file

path = '/FileStore/data/file.parquet'
df = spark.read.parquet(path=path)

# COMMAND ----------

# Reading all parquet files in a folder
path = '/FileStore/data/parquet/'
df = spark.read.parquet(path=path)