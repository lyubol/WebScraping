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
  ActivitiesTimePercent0 INT,
  Activities0            STRING,
  ActivitiesTimePercent1 INT,
  Activities1            STRING,
  ActivitiesTimePercent2 INT,
  Activities2            STRING,
  ActivitiesTimePercent3 INT,
  Activities3            STRING,
  ActivitiesTimePercent4 INT,
  Activities4            STRING,
  ActivitiesTimePercent5 INT,
  Activities5            STRING,
  ActivitiesTimePercent6 INT,
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

-- DELETE FROM WAREHOUSE.DimActivities
-- WHERE ActivitiesId IN (156, 833)

-- UPDATE WAREHOUSE.DimActivities
-- SET ActivitiesTimePercent0 = 101
-- WHERE ActivitiesId = 141

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


