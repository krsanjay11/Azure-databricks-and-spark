# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest constructors.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "") 
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-04-18") 
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read json file using spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING" 

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 -Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col
constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 3 - Rename columns and add ingestion date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id")\
                                             .withColumnRenamed("constructorRef", "constructor_ref")\
                                             .withColumn("data_source", lit(v_data_source)) \
                                             .withColumn("ingestion_date", current_timestamp()) \
                                             .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### step 4 - write output to delta file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors;

# COMMAND ----------

df = spark.read.format("delta").load(f"{processed_folder_path}/constructors")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date, count(1) from f1_processed.constructors group by file_date;

# COMMAND ----------

dbutils.notebook.exit("success")