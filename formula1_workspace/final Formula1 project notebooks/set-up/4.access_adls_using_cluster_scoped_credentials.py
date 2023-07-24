# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using cluster scoped credentials
# MAGIC 1. set the spark config fs.azure.account.key.formula1skdl.dfs.core.windows.net 
# MAGIC 2. List files from deo container
# MAGIC 3. Read data from circuit.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1skdl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1skdl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

