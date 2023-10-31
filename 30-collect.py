# Databricks notebook source
# MAGIC %md
# MAGIC Collect retrieves all elements in a df as an array of rows  
# MAGIC Best practice is to use collect with small dfs

# COMMAND ----------

# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df = create_standard_df()

# COMMAND ----------

df_rows = df.collect()
print(df_rows)

# COMMAND ----------

# Accessing first row
print(df_rows[0])

# COMMAND ----------

# Looping through all rows
for row in df_rows:
  print(row)

# COMMAND ----------

# Example to get the id and name of people that have a salary equal to or greater than 10.000
people_list = df.select(df.name, df.id).filter(df.salary >= 10000).collect()
people = list()

for p in people_list:
  person = {
    'name': p[0],
    'id': p[1]
  }

  people.append(person)

print(people)