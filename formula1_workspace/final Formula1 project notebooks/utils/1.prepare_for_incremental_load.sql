-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Drop all the tables
-- MAGIC

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1skdl/processed";

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1skdl/presentation";

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_demo CASCADE;

-- COMMAND ----------

