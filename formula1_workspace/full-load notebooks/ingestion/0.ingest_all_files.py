# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

b1_result = dbutils.notebook.run("1.ingest_circuits_file", 0, {"p_data_source": "Ergast API"})
b1_result

# COMMAND ----------

b2_result = dbutils.notebook.run("2.ingest_races_file", 0, {"p_data_source": "Ergast API"})
b2_result

# COMMAND ----------

b3_result = dbutils.notebook.run("3.ingest_constructors_file", 0, {"p_data_source": "Ergast API"})
b3_result

# COMMAND ----------

b4_result = dbutils.notebook.run("4.ingest_drivers_file", 0, {"p_data_source": "Ergast API"})
b4_result

# COMMAND ----------

b5_result = dbutils.notebook.run("5.ingest_results_file", 0, {"p_data_source": "Ergast API"})
b5_result

# COMMAND ----------

b6_result = dbutils.notebook.run("6.ingest_pit_stops_file", 0, {"p_data_source": "Ergast API"})
b6_result

# COMMAND ----------

b7_result = dbutils.notebook.run("7.ingest_lap_times_file", 0, {"p_data_source": "Ergast API"})
b7_result

# COMMAND ----------

b8_result = dbutils.notebook.run("8.ingest_qualifying_file", 0, {"p_data_source": "Ergast API"})
b8_result