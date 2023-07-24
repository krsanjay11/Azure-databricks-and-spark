# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 1 - Read CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
circuit_schema = StructType(fields=[StructField("circuitId", IntegerType(), False), 
                                    StructField("circuitRef", StringType(), True), 
                                    StructField("name", StringType(), True), 
                                    StructField("location", StringType(), True), 
                                    StructField("country", StringType(), True), 
                                    StructField("lat", DoubleType(), True), 
                                    StructField("lng", DoubleType(), True), 
                                    StructField("alt", IntegerType(), True), 
                                    StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read.option("header", True).schema(circuit_schema).csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 2 - Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col, lit
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 3 - Rename the columns as required
# MAGIC

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id")\
    .withColumnRenamed("alt", "altitute")\
    .withColumnRenamed("circuitRef", "circuit_ref")\
    .withColumnRenamed("lat", "latitude")\
    .withColumnRenamed("lng", "longitude")\
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Add ingestion date to the dataframe
# MAGIC

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - write data to datalake as parquet
# MAGIC

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

df = spark.read.format("delta").load(f"{processed_folder_path}/circuits")
display(df)

# COMMAND ----------

# MAGIC %sql 
# MAGIC Select * from f1_processed.circuits;

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date, count(1) from f1_processed.circuits group by file_date;

# COMMAND ----------

dbutils.notebook.exit("success")