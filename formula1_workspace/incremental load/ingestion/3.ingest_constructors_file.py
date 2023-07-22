# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest constructors.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "") # "" is default
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21") # "" is default
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read json file using spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING" # Datatype comes from hive

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# constructor_df.printSchema()

# COMMAND ----------

# display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 -Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# display(constructor_dropped_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 3 - Rename columns and add ingestion date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id")\
                                             .withColumnRenamed("constructorRef", "constructor_ref")\
                                             .withColumn("data_source", lit(v_data_source)) \
                                             .withColumn("ingestion_date", current_timestamp()) \
                                             .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### step 4 - write output to parquet file

# COMMAND ----------

# constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors;

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/constructors"))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1skdl/processed/constructors

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

