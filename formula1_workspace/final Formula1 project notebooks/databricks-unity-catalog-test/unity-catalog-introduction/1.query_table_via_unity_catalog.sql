-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### METHOD 1 - Query data via unity catalog using 3 level namespace

-- COMMAND ----------

SELECT * FROM demo_catalog.demo_schema.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### METHOD 2 - Query data via unity catalog using USE 

-- COMMAND ----------

SELECT current_catalog()

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

SELECT current_schema()

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

USE demo_catalog.demo_schema;

-- COMMAND ----------

USE CATALOG demo_catalog;
USE SCHEMA demo_schema;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### METHOD 3 - Query data via unity catalog using PYTHON

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql('SHOW TABLES;'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table('demo_catalog.demo_schema.circuits')
-- MAGIC display(df)

-- COMMAND ----------

