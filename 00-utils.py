# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

data_df = [
  {'id': 1, 'name': 'Ricardo', 'salary': 12000.0, 'gender': 'm', 'department_id': 2, 'manager_id': 0},
  {'id': 2, 'name': 'Jordana', 'salary': 15000.0, 'gender': 'f', 'department_id': 3, 'manager_id': 1},
  {'id': 7, 'name': 'Beatriz', 'salary': 13000.0, 'gender': 'f', 'department_id': 3, 'manager_id': 1},
  {'id': 3, 'name': 'Lucas', 'salary': 5000.0, 'gender': 'm', 'department_id': 2, 'manager_id': 2},
  {'id': 4, 'name': 'Rodrigo', 'salary': 4000.0, 'gender': 'm', 'department_id': 1, 'manager_id': 0},
  {'id': 6, 'name': 'Rodrigo', 'salary': 4000.0, 'gender': 'm', 'department_id': 1, 'manager_id': 1},
  {'id': 8, 'name': 'Lais', 'salary': 8000.0, 'gender': 'm', 'department_id': 3, 'manager_id': 1},
  {'id': 5, 'name': 'João', 'salary': 25000.0, 'gender': 'm', 'department_id': 4, 'manager_id': 2},
  {'id': 9, 'name': 'Gabriel', 'salary': 25000.0, 'gender': 'm', 'department_id': 7, 'manager_id': 4},
  {'id': None, 'name': None, 'salary': 25000.0, 'gender': None, 'department_id': None, 'manager_id': None}
]

schema = StructType().add(field='id', data_type=IntegerType(), nullable=True) \
                       .add(field='name', data_type=StringType(), nullable=True) \
                       .add(field='salary', data_type=DoubleType(), nullable=True) \
                       .add(field='gender', data_type=StringType(), nullable=True) \
                       .add(field='department_id', data_type=StringType(), nullable=True) \
                       .add(field='manager_id', data_type=StringType(), nullable=True) 

data_dup = [
  {'id': 1, 'name': 'Ricardo', 'salary': 12000.0, 'gender': 'm'},
  {'id': 1, 'name': 'Ricardo', 'salary': 12000.0, 'gender': 'm'},
  {'id': 2, 'name': 'Jordana', 'salary': 15000.0, 'gender': 'f'},
  {'id': 7, 'name': 'Beatriz', 'salary': 15000.0, 'gender': 'f'},
  {'id': 3, 'name': 'Lucas', 'salary': 5000.0, 'gender': 'm'},
  {'id': 3, 'name': 'Lucas', 'salary': 5000.0, 'gender': 'm'},
  {'id': 4, 'name': 'Rodrigo', 'salary': 4000.0, 'gender': 'm'},
  {'id': 6, 'name': 'Rodrigo', 'salary': 4000.0, 'gender': 'm'},
  {'id': 5, 'name': 'João', 'salary': 25000.0, 'gender': 'm'},
]

schema_dup = StructType().add(field='id', data_type=IntegerType(), nullable=True) \
                            .add(field='name', data_type=StringType(), nullable=True) \
                            .add(field='salary', data_type=DoubleType(), nullable=True) \
                            .add(field='gender', data_type=StringType(), nullable=True)


def create_standard_df(type_df='normal'):
  if type_df == 'dup':
    df = spark.createDataFrame(data_dup, schema=schema_dup)
  else:
    df = spark.createDataFrame(data_df, schema=schema)

  return df

# COMMAND ----------

departments = [
  {'id': 1, 'name': 'IT'},
  {'id': 2, 'name': 'Data'},
  {'id': 3, 'name': 'HR'},
  {'id': 4, 'name': 'BP'},
  {'id': 5, 'name': 'Infra'},
]

def create_departments_df():
  schema = StructType().add(field='id', data_type=IntegerType()) \
                       .add(field='name', data_type=StringType())

  df = spark.createDataFrame(departments, schema)
  return df

# COMMAND ----------

data_rdd = [(1, 'Ricardo'), (2, 'Jordana')]

def create_basic_rdd(data=data_rdd):
  rdd = spark.sparkContext.parallelize(data)
  return rdd