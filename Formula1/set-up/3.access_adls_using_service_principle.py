# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service Principle
# MAGIC #####1.Register Azure AD Application/ Service principle.
# MAGIC #####2.Generate a secret password for application.
# MAGIC #####3. Set spark config with App/Client ID, Directory/Tenant ID & Secret.
# MAGIC #####4. Assign Role 'Storage Blob Data Contributor' to the Data Lake. 
# MAGIC

# COMMAND ----------

client_id = "d6279eea-36b2-411e-b3e4-b9cc2f5fcd14"
tenant_id = "ce06cf9a-e908-424b-aa1d-25072f187cae"
client_secret = "fkQ8Q~6TN~R.mYO5Bw-IUbVTmhXq6YwI_L3Xhdqp"
## client secret is the secret genereted in the service principle 

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1datalake16.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1datalake16.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.f1datalake16.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1datalake16.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1datalake16.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1datalake16.dfs.core.windows.net"))

# COMMAND ----------

df1=spark.read.csv("abfss://demo@f1datalake16.dfs.core.windows.net")
display(df1)
