# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using service principal
# MAGIC 1. Register Azure AD application / service principal
# MAGIC 2. generate a secret / password for the application
# MAGIC 3. set spark config with app/ client id, directory/ tenant id & secret
# MAGIC 4. assig role 'storage blob data contributor to the data lake.

# COMMAND ----------

# client_id = "60b7ff0c-a299-46b9-bc0c-e86f7b79544b"
# tenant_id = "3b763e8e-1cc7-4b64-8586-546eec953a6b"
# client_secret = "HdD8Q~gSMYgGSwoNHOhJw3t~FvHwBoQVF14QObNv"

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1skdl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1skdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1skdl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1skdl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1skdl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1skdl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1skdl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

