# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore the capabilites of dbutils.secrets.utility 
# MAGIC

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope ='formula1scope')

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(scope='formula1scope',key='formula1dl-accesskey')

# COMMAND ----------

spark.conf.set("fs.azure.account.key.f1datalake16.dfs.core.windows.net",
               formula1dl_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1datalake16.dfs.core.windows.net"))

# COMMAND ----------

df1=spark.read.csv("abfss://demo@f1datalake16.dfs.core.windows.net")
display(df1)
