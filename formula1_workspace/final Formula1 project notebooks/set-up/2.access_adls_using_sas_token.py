# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using SAS token
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formula1dl_demo_sas_token = dbutils.secrets.get(scope='formula1-scope', key= 'formula1dl-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1skdl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1skdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
# spark.conf.set("fs.azure.sas.fixed.token.formula1skdl.dfs.core.windows.net", "sp=rl&st=2023-07-10T14:41:57Z&se=2023-07-10T22:41:57Z&spr=https&sv=2022-11-02&sr=c&sig=h8Lwgnu%2Bu4FngtUWarTly47%2BCP18%2Bmwo63UqXozognU%3D")
spark.conf.set("fs.azure.sas.fixed.token.formula1skdl.dfs.core.windows.net", formula1dl_demo_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1skdl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1skdl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

