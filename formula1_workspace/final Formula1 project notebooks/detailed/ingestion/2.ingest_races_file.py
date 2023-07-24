# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest races.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "") # "" is default
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-04-18") # "" is default
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read.option("header", True).schema(races_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add ingestion data and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, lit, concat, col

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                                                 .withColumn("data_source", lit(v_data_source)) \
                                                 .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

races_with_timestamp_df = add_ingestion_date(races_with_timestamp_df)

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - select only the columns required and rename as required

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias("race_id"), col('year').alias("race_year"), col('circuitId').alias("circuit_id"), col('round'), col('name'), col('race_timestamp'), col('data_source'), col('file_date'), col('ingestion_date'))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### write the output to processed container in parquet

# COMMAND ----------

# races_selected_df.write.mode('overwrite').parquet(f"{processed_folder_path}/races")

# COMMAND ----------

# races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet(f"{processed_folder_path}/races")

# COMMAND ----------

# races_selected_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/races"))

# COMMAND ----------

races_selected_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races;

# COMMAND ----------

df = spark.read.format("delta").load(f"{processed_folder_path}/races")
display(df)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1skdl/processed/races

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

