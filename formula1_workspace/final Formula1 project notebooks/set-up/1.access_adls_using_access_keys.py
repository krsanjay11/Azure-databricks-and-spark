# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-account-key')
print(formula1dl_account_key)

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1skdl.dfs.core.windows.net", 
    formula1dl_account_key
    # "j1vhT1DMtKT/y1TkgjgjQj4qNCN8T2iabUe7UWVzPyebtEr01R5OZO44BHrG7CdUrKgd6oCb7kfs+AStm9BNxQ=="
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1skdl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1skdl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

