-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## WAREHOUSE Database Definition

-- COMMAND ----------

-- DBTITLE 1,Create WAREHOUSE database
CREATE DATABASE IF NOT EXISTS WAREHOUSE
LOCATION '/mnt/adlslirkov/it-job-boards/Warehouse/Database/'
