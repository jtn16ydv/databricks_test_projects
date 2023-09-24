# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC ######1.Set the spark config SAS Token
# MAGIC ######2.List files from demo container
# MAGIC ######3.Read data from circuits.csv file
# MAGIC

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.list(scope='formula1scope')

# COMMAND ----------

sas_Token = dbutils.secrets.get(scope='formula1scope', key='formula1dl-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1datalake16.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.f1datalake16.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.f1datalake16.dfs.core.windows.net", sas_Token)

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1datalake16.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("")

# COMMAND ----------

df1=spark.read.csv("abfss://demo@f1datalake16.dfs.core.windows.net")
display(df1)
