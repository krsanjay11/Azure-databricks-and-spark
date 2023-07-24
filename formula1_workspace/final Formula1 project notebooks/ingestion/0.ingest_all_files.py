# Databricks notebook source
b1_result = dbutils.notebook.run("1.ingest_circuits_file", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-03-21"})
b1_result

# COMMAND ----------

b2_result = dbutils.notebook.run("2.ingest_races_file", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-03-21"})
b2_result

# COMMAND ----------

b3_result = dbutils.notebook.run("3.ingest_constructors_file", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-03-21"})
b3_result

# COMMAND ----------

b4_result = dbutils.notebook.run("4.ingest_drivers_file", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-03-21"})
b4_result

# COMMAND ----------

b5_result = dbutils.notebook.run("5.ingest_results_file", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-03-21"})
b5_result

# COMMAND ----------

b6_result = dbutils.notebook.run("6.ingest_pit_stops_file", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-03-21"})
b6_result

# COMMAND ----------

b7_result = dbutils.notebook.run("7.ingest_lap_times_file", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-03-21"})
b7_result

# COMMAND ----------

b8_result = dbutils.notebook.run("8.ingest_qualifying_file", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-03-21"})
b8_result