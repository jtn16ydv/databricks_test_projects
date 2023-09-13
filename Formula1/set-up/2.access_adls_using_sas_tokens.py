# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC ######1.Set the spark config SAS Token
# MAGIC ######2.List files from demo container
# MAGIC ######3.Read data from circuits.csv file
# MAGIC

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1datalake16.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.f1datalake16.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.f1datalake16.dfs.core.windows.net", "sp=rl&st=2023-09-12T10:47:33Z&se=2023-09-12T18:47:33Z&spr=https&sv=2022-11-02&sr=c&sig=Zc1oXNVu45Szu9w%2FudaYclGuQ%2Fb7HMzSZQDXCJnwj5o%3D")

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1datalake16.dfs.core.windows.net"))

# COMMAND ----------

df1=spark.read.csv("abfss://demo@f1datalake16.dfs.core.windows.net")
display(df1)
