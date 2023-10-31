# Databricks notebook source
# MAGIC %run "/yt-course-india/00-utils"

# COMMAND ----------

df_emp = create_standard_df()
df_dep = create_departments_df()

# COMMAND ----------

df = df_emp.join(other=df_dep, on=df_dep.id == df_emp.department_id, how='left') \
            .select(
              df_emp.id,
              df_emp.name,
              df_emp.gender,
              df_dep.name.alias('department')
            )

display(df)

# COMMAND ----------

# Filling NULL/None values
# We can pass a list of columns as an argument to fill multiple columns
# If we don't specify a column, all columns with the same data type as the value used to replace, will be replaced. Ex: If we pass a string, all string columns will be replaced

value_replace = 'N/A'

display(
  df.na.fill(value_replace, ['gender'])
)

cols = ['gender', 'department']
display(
  df.fillna(value_replace, cols)
)

# All string columns
display(
  df.fillna(value_replace) \
    .fillna(-1) # To replace id column that is integer type
)

# Using a dict
to_replace = {
  'id': -1,
  'name': 'Unknown',
  'gender': 'Unknown',
  'department': 'N/A',
}

display(
  df.fillna(to_replace)
)

# COMMAND ----------

# Filtering out null values
df_emp = df_emp.filter(df_emp['id'].isNotNull())