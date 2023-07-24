-- Databricks notebook source
use f1_processed;

-- COMMAND ----------

select file_date, count(1) from f1_processed.circuits group by file_date;

-- COMMAND ----------

select file_date, count(1) from f1_processed.races group by file_date;

-- COMMAND ----------

select file_date, count(1) from f1_processed.constructors group by file_date;

-- COMMAND ----------

select file_date, count(1) from f1_processed.drivers group by file_date;

-- COMMAND ----------

select file_date, count(1) from f1_processed.results group by file_date;

-- COMMAND ----------

select file_date, count(1) from f1_processed.pit_stops group by file_date;

-- COMMAND ----------

select file_date, count(1) from f1_processed.lap_times group by file_date;

-- COMMAND ----------

select file_date, count(1) from f1_processed.qualifying group by file_date;

-- COMMAND ----------

select file_date, count(1) from f1_presentation.race_results group by file_date;

-- COMMAND ----------

select race_year, count(1) 
from f1_presentation.driver_standings
group by race_year
order by race_year desc;

-- COMMAND ----------

select race_year, count(1) 
from f1_presentation.constructor_standings
group by race_year
order by race_year desc;

-- COMMAND ----------

select race_year, count(1) 
from f1_presentation.calculated_race_results
group by race_year
order by race_year desc;

-- COMMAND ----------

