# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake using Service Principle
# MAGIC #####1.Get client_id, tenant_id and client_secrets from key vault.
# MAGIC #####2.Set Spark Config with App/Client Id,Directory/Tenant Id Secret.
# MAGIC #####3.Call the file sytem utility mount to mount storage .
# MAGIC #####4.Explore ther file system utilities related to mount (list all mounts, unmounts)

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula1scope",key="formula1-app-clientid")
tenant_id = dbutils.secrets.get(scope="formula1scope",key="formula1-app-tenant-id")
client_secret = dbutils.secrets.get(scope="formula1scope",key="formula1-app-client-secret")
## client secret is the secret genereted in the service principle 

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@f1datalake16.dfs.core.windows.net/",
  mount_point = "/mnt/f1datalake16/demo",
  extra_configs = configs)

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1datalake16.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1datalake16.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.f1datalake16.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1datalake16.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1datalake16.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

df1 = spark.read.csv("/mnt/f1datalake16/demo/circuits.csv")
display(df1)

# COMMAND ----------

# to see where the datalake has been mounted
dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.unmount("")
#to unmount the data storage

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/f1datalake16/demo/circuits"))
