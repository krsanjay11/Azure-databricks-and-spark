-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### lesson objectives
-- MAGIC 1. Spark SQL documentation
-- MAGIC 2. Create database demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW command
-- MAGIC 5. DESCRIBE command
-- MAGIC 6. Find the current database

-- COMMAND ----------

CREATE DATABASE demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW databases;

-- COMMAND ----------

DESC DATABASE EXTENDED f1_raw;

-- COMMAND ----------

DESC DATABASE EXTENDED f1_presentation;

-- COMMAND ----------

DESC DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN f1_raw;

-- COMMAND ----------

SHOW TABLES IN default;

-- COMMAND ----------

USE f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Learning objectives
-- MAGIC 1. Create managed table using python 
-- MAGIC 2. Create managed table using SQL
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create managed table using python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

select * from demo.race_results_python where race_year = 2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create managed table using sql

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS 
select * from demo.race_results_python where race_year = 2020;

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Learning objectives
-- MAGIC 1. Create EXTERNAL table using python 
-- MAGIC 2. Create EXTERNAL table using SQL
-- MAGIC 3. Effect of dropping a EXTERNAL table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df.write.format("parquet").option("path", f"{presentation_foler_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py;

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP,
)
USING parquet
LOCATION "/mnt/formula1skdl/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_resutls_ext_py WHERE race_year = 2020;

-- COMMAND ----------

select count(1) FROM demo.race_results_ext_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Views on tables
-- MAGIC #### Learning objectives
-- MAGIC 1. Create temp view
-- MAGIC 2. Create global temp view
-- MAGIC 3. create permanent view

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2018;

-- COMMAND ----------

select * from v_race_results;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2018;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

select * from global_temp.gv_race_results;

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2018;

-- COMMAND ----------

select * from demo.pv_race_results;