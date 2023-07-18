# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest lap_times folder

# COMMAND ----------

dbutils.widgets.text("p_data_source", "") # "" is default
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 1- Read the csv file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

# display(lap_times_df)

# COMMAND ----------

# lap_times_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 2- Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
                       .withColumnRenamed("raceId", "race_id") \
                       .withColumn("data_source", lit(v_data_source)) \
                       .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 3 - write to output to processed container in parquet format

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.lap_times;

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("success")