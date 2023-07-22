-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1skdl/processed" -- creating managed tables

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------

-- DROP DATABASE IF EXISTS f1_processed;