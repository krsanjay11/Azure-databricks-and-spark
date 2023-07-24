# Databricks notebook source
# MAGIC %md
# MAGIC # Explore DBFS Root
# MAGIC 1. list all the folders in DBFS root
# MAGIC 2. interact with DBFS file browser
# MAGIC 3. upload file to DBFS Root

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

