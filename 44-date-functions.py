# Databricks notebook source
from pyspark.sql.functions import current_date, date_format, to_date, datediff, months_between, add_months, date_add, year, month, dayofmonth

# COMMAND ----------

df = spark.range(2)

# COMMAND ----------

# Adding column with current date
df = df.withColumn('current_date', current_date())
display(df)

# COMMAND ----------

# Converting date to string
df = df.withColumn('br_date', date_format(df.current_date, 'dd/MM/yyyy'))  # It generates a string
display(df)

# COMMAND ----------

# Converting string to date. We need to specify the format of the string column
df = df.withColumn('new_date', to_date(df.br_date, 'dd/MM/yyyy'))
display(df)

# COMMAND ----------

from pyspark.sql.functions import lit

# Changing value of new_date column to use other date functions
df = df.withColumn('new_date', lit('1997-09-03').cast('Date'))

# COMMAND ----------

# Difference of days between two dates
df = df.withColumn('datediff', datediff(df.current_date, df.new_date))
display(df)

# COMMAND ----------

# Difference of months between two dates
df = df.withColumn('months_between', months_between(df.current_date, df.new_date))
display(df)

# COMMAND ----------

# Add months to date
df = df.withColumn('add_months', add_months(df.new_date, 5))  # We can also subtract months passing a negative value
display(df)

# COMMAND ----------

# Adding days to a date
df = df.withColumn('date_add', date_add(df.new_date, 7))  # Same as previous function, we can pass a negative value to subtract days
display(df)

# COMMAND ----------

# Extracting year, month and day from date
df = df.withColumn('year', year(df.new_date)) \
      .withColumn('month', month(df.new_date)) \
      .withColumn('day', dayofmonth(df.new_date)) \

display(df)