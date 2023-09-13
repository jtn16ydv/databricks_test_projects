# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Access Keys
# MAGIC ######1.Set the spark config fs.azure.account.key
# MAGIC ######2.List files from demo container
# MAGIC ######3.Read data from circuits.csv file
# MAGIC

# COMMAND ----------

spark.conf.set("fs.azure.account.key.f1datalake16.dfs.core.windows.net",
               "nDc6R2C+Jmx0smYdjVSvwai7SdS8+83KhM6GkTPi3CdnwiaXCQMrvpTar8RODB8djwtPiMcUKGx4+AStcnazHA==")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1datalake16.dfs.core.windows.net"))

# COMMAND ----------

df1=spark.read.csv("abfss://demo@f1datalake16.dfs.core.windows.net")
display(df1)
