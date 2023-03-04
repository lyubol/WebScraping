-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## WAREHOUSE Database

-- COMMAND ----------

-- DBTITLE 1,Create WAREHOUSE database
CREATE DATABASE IF NOT EXISTS WAREHOUSE
LOCATION '/mnt/adlslirkov/it-job-boards/Warehouse/Database/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimActivities

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimAwards

-- COMMAND ----------

-- DBTITLE 1,DimAwards
CREATE TABLE WAREHOUSE.DimAwards (
  AwardsKey BIGINT GENERATED ALWAYS AS IDENTITY,
  AwardsId BIGINT NOT NULL,
  SourceSystem STRING NOT NULL,
  Awards0 STRING,
  Awards1 STRING,
  Awards2 STRING,
  Awards3 STRING,
  Awards4 STRING,
  Awards5 STRING,
  Awards6 STRING,
  Awards7 STRING,
  Awards8 STRING,
  IngestionDate TIMESTAMP,
  IsActive BOOLEAN,
  StartDate TIMESTAMP,
  EndDate TIMESTAMP
)
USING DELTA OPTIONS (path '/mnt/adlslirkov/it-job-boards/Warehouse/DimAwards')
TBLPROPERTIES ('external.table.purge'='true')
COMMENT 'The Awards dimension'

-- COMMAND ----------

DROP TABLE WAREHOUSE.DimAwards

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("/mnt/adlslirkov/it-job-boards/Warehouse/DimAwards", True)

-- COMMAND ----------

SELECT * FROM WAREHOUSE.DimAwards

-- DELETE FROM WAREHOUSE.DimAwards
-- WHERE AwardsId IN (171, 27)

-- UPDATE WAREHOUSE.DimAwards
-- SET Awards0 = '12M+ Monthly Readers'
-- WHERE AwardsId = 171

-- COMMAND ----------


