-- Databricks notebook source
-- DBTITLE 1,Create database 'WAREHOUSE'
CREATE DATABASE IF NOT EXISTS WAREHOUSE
COMMENT 'This is the Warehouse database that holds the dimensional model of the it-job-boards data.'
LOCATION '/mnt/adlslirkov/it-job-boards/Warehouse/Database/'

-- COMMAND ----------

SHOW DATABASES
