# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file

# COMMAND ----------

# dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "") # "" is default
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# print(raw_folder_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 1 - Read CSV file using the spark dataframe reader

# COMMAND ----------

# display(dbutils.fs.mounts())

# COMMAND ----------

# %fs
# ls /mnt/formula1skdl/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

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

circuits_df = spark.read.option("header", True).schema(circuit_schema).csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

# type(circuits_df) # pyspark.sql.dataframe.DataFrame

# COMMAND ----------

# circuits_df.show() # show the records
# display(circuits_df) # show in table format

# COMMAND ----------

# circuits_df.printSchema()

# COMMAND ----------

# circuits_df.describe().show() # basic calculation like min, max, count, stddev, mean

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 2 - Select only the required columns

# COMMAND ----------

# circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

# circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)

# COMMAND ----------

# circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 3 - Rename the columns as required
# MAGIC

# COMMAND ----------

circuits_renamed_df = circuits_df.select(col("circuitId").alias("circuit_id"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id")\
    .withColumnRenamed("alt", "altitute")\
    .withColumnRenamed("circuitRef", "circuit_ref")\
    .withColumnRenamed("lat", "latitude")\
    .withColumnRenamed("lng", "longitude")\
    .withColumn("data_source", lit(v_data_source)) # convert text variable to columntype

# COMMAND ----------

# display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Add ingestion date to the dataframe
# MAGIC

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)
# .withColumn("env", lit("Production"))

# COMMAND ----------

# display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - write data to datalake as parquet
# MAGIC

# COMMAND ----------

# circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended f1_processed.circuits;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1skdl/processed/circuits

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/circuits")
display(df)

# COMMAND ----------

dbutils.notebook.exit("success")