# Databricks notebook source
# MAGIC %md
# MAGIC ## Access dataframes using SQL
# MAGIC valid only for current spark session
# MAGIC #### Objectives
# MAGIC 1. creating temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results") # camelcase

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from v_race_results where race_year = 2020

# COMMAND ----------

p_race_year = 2019

# COMMAND ----------

race_results_2019_df = spark.sql(f"Select * from v_race_results where race_year = {p_race_year}")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Global temporary views
# MAGIC 1. creating Global temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from python cell
# MAGIC 4. Access the view from another notebook
# MAGIC

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from global_temp.gv_race_results where race_year = 2020

# COMMAND ----------

race_results_2019_df = spark.sql(f"Select * from global_temp.gv_race_results where race_year = {p_race_year}").show()

# COMMAND ----------

