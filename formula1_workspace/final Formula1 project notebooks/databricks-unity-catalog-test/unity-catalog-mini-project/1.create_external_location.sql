-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create the external locations required for this project

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricksexternaldl_bronze
URL "abfss://bronze@formula1skdl.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `databrickscourse-ext-storage-credential`)

-- COMMAND ----------

DESC EXTERNAL LOCATION databricksexternaldl_bronze;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls "abfss://bronze@formula1skdl.dfs.core.windows.net/"

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricksexternaldl_gold
URL "abfss://gold@formula1skdl.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `databrickscourse-ext-storage-credential`)

-- COMMAND ----------

DESC EXTERNAL LOCATION databricksexternaldl_gold;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls "abfss://gold@formula1skdl.dfs.core.windows.net/"

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricksexternaldl_silver
URL "abfss://silver@formula1skdl.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `databrickscourse-ext-storage-credential`)

-- COMMAND ----------

DESC EXTERNAL LOCATION databricksexternaldl_silver;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls "abfss://silver@formula1skdl.dfs.core.windows.net/"

-- COMMAND ----------

