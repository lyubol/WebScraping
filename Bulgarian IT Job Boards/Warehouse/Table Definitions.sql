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

-- DBTITLE 1,DimActivities Table Definition
CREATE TABLE WAREHOUSE.DimActivities (
  ActivitiesKey          BIGINT GENERATED ALWAYS AS IDENTITY,
  ActivitiesId           BIGINT NOT NULL,
  SourceSystem           STRING NOT NULL,
  ActivitiesTimePercent0 STRING,
  Activities0            STRING,
  ActivitiesTimePercent1 STRING,
  Activities1            STRING,
  ActivitiesTimePercent2 STRING,
  Activities2            STRING,
  ActivitiesTimePercent3 STRING,
  Activities3            STRING,
  ActivitiesTimePercent4 STRING,
  Activities4            STRING,
  ActivitiesTimePercent5 STRING,
  Activities5            STRING,
  ActivitiesTimePercent6 STRING,
  Activities6            STRING,
  IngestionDate          TIMESTAMP,
  IsActive               BOOLEAN,
  StartDate              TIMESTAMP,
  EndDate                TIMESTAMP
)
USING DELTA OPTIONS (path '/mnt/adlslirkov/it-job-boards/Warehouse/DimActivities')
TBLPROPERTIES ('external.table.purge'='true')
COMMENT 'The Activities dimension'

-- COMMAND ----------

DROP TABLE WAREHOUSE.DimActivities

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("/mnt/adlslirkov/it-job-boards/Warehouse/DimActivities", True)

-- COMMAND ----------

SELECT * FROM WAREHOUSE.DimActivities

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimAwards

-- COMMAND ----------

-- DBTITLE 1,DimAwards Table Definition
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

SELECT * FROM WAREHOUSE.DimActivities

-- COMMAND ----------


