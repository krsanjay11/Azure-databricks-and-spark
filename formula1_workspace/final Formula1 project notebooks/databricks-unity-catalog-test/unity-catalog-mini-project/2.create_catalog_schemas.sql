-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Create catalogs and schemas required for the project

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS formula1_dev;

-- COMMAND ----------

USE CATALOG formula1_dev;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS bronze
MANAGED LOCATION "abfss://bronze@formula1skdl.dfs.core.windows.net/";

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS silver
MANAGED LOCATION "abfss://silver@formula1skdl.dfs.core.windows.net/";

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS gold
MANAGED LOCATION "abfss://gold@formula1skdl.dfs.core.windows.net/";

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

