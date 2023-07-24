-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM drivers LIMIT 10;

-- COMMAND ----------

DESC drivers;

-- COMMAND ----------

SELECT * FROM drivers WHERE nationality = 'British';

-- COMMAND ----------

select nationality, name, dob, RANK() over (partition by nationality order by dob desc) as age_rank
from drivers order by nationality, age_rank

-- COMMAND ----------

