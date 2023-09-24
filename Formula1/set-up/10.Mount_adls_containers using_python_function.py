# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake using Python function

# COMMAND ----------

def mountUtils(storage_account_name,container_name):
    # Get secrets from Key Vault
    client_id = dbutils.secrets.get(scope="formula1scope",key="formula1-app-clientid")
    tenant_id = dbutils.secrets.get(scope="formula1scope",key="formula1-app-tenant-id")
    client_secret = dbutils.secrets.get(scope="formula1scope",key="formula1-app-client-secret")
## client secret is the secret genereted in the service principle 
# 
# Set Spark Configurations 
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    # Mounting data Lake
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs) 
    


# COMMAND ----------

mountUtils("f1datalake16","presentation")
mountUtils("f1datalake16","processed")
mountUtils("f1datalake16","raw")

# COMMAND ----------

dbutils.fs.ls("/mnt/f1datalake16/presentation")

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
