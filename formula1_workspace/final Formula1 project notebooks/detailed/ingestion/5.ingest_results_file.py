# Databricks notebook source
# spark.read.json("/mnt/formula1skdl/raw/2021-03-21/results.json").createOrReplaceTempView("results_cutover")
# spark.read.json("/mnt/formula1skdl/raw/2021-04-18/results.json").createOrReplaceTempView("results_w2")
# spark.read.json("/mnt/formula1skdl/raw/2021-03-28/results.json").createOrReplaceTempView("results_w1")

# COMMAND ----------

# %sql
# select raceId, count(1)
# from results_cutover
# group by raceId
# order by raceId desc;

# COMMAND ----------

# %sql
# select raceId, count(1)
# from results_w1
# group by raceId
# order by raceId desc;

# COMMAND ----------

# %sql
# select raceId, count(1)
# from results_w2
# group by raceId
# order by raceId desc;

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingest results.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "") # "" is default
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21") # "" is default
v_file_date = dbutils.widgets.get("p_file_date")
print(v_file_date)

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 1 - Read the json file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)
])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 2 Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_Text") \
                                    .withColumnRenamed("postionOrder", "postion_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 3 - drop the unwanted column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### De-dup the dataframe

# COMMAND ----------

results_dedupted_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

print(results_dedupted_df)

# COMMAND ----------

results_final_df = results_dedupted_df

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE f1_processed.results;

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 4 - write the output to processed container in parquet format

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method - 1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect(): # convert this into list
#     # print(race_id_list.race_id)
#     if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method - 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE f1_processed.results;

# COMMAND ----------

# results_final_df = results_final_df.select("resultId", "driverId", "constructorId", "number", "grid", "position", "positionText", "positionOrder", "points", "laps", "time", "milliseconds", "fastestLap", "rank", "fastestLapTime", "fastestLapSpeed", "data_source", "file_date", "ingestion_date", "race_id")

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
# if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     results_final_df.write.mode("overwrite").insertInto("f1_processed.results") # partition should be the last column
# else:
#     results_final_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# results_final_df = re_arrange_partition_column(results_final_df, "race_id")
# display(results_final_df)

# COMMAND ----------

# overwrite_partition(results_final_df, "f1_processed", "results", "race_id")

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Convert to delta lake format

# COMMAND ----------

# spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

# from delta.tables import DeltaTable
# if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     deltaTable = DeltaTable.forPath(spark, "/mnt/formula1skdl/processed/results")
#     deltaTable.alias("tgt").merge(
#         results_final_df.alias("src"),
#         "tgt.result_id = src.result_id AND tgt.race_id = src.race_id")\
#     .whenMatchedUpdateAll()\
#     .whenNotMatchedInsertAll()\
#     .execute()
# else:
#     results_final_df.write.mode("overwrite").partitionBy("race_id").format("delta").saveAsTable("f1_processed.results")

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_final_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

df = spark.read.format("delta").load(f"{processed_folder_path}/results")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results;

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from f1_processed.results;

# COMMAND ----------

# %sql
# SELECT race_id, driver_id, COUNT(1)
# FROM f1_processed.results
# GROUP BY race_id, driver_id
# HAVING COUNT(1) > 1
# ORDER BY race_id, driver_id DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.results WHERE race_id=540 AND driver_id = 229;

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

