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

-- MAGIC %md
-- MAGIC ## DimBenefits

-- COMMAND ----------

-- DBTITLE 1,DimBenefits Table Definition
CREATE TABLE WAREHOUSE.DimBenefits (
  BenefitsKey          BIGINT GENERATED ALWAYS AS IDENTITY,
  BenefitsId           BIGINT NOT NULL,
  SourceSystem         STRING NOT NULL,
  Benefits0            STRING,
  Benefits1            STRING,
  Benefits2            STRING,
  Benefits3            STRING,
  Benefits4            STRING,
  Benefits5            STRING,
  Benefits6            STRING,
  Benefits7            STRING,
  Benefits8            STRING,
  Benefits9            STRING,
  Benefits10           STRING,
  Benefits11           STRING,
  Benefits12           STRING,
  Benefits13           STRING,
  Benefits14           STRING,
  Benefits15           STRING,
  Benefits16           STRING,
  Benefits17           STRING,
  Benefits18           STRING,
  IngestionDate        TIMESTAMP,
  IsActive             BOOLEAN,
  StartDate            TIMESTAMP,
  EndDate              TIMESTAMP
)
USING DELTA OPTIONS (path '/mnt/adlslirkov/it-job-boards/Warehouse/DimBenefits')
TBLPROPERTIES ('external.table.purge'='true')
COMMENT 'The Benefits dimension'

-- COMMAND ----------

DROP TABLE WAREHOUSE.DimBenefits

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("/mnt/adlslirkov/it-job-boards/Warehouse/DimBenefits", True)

-- COMMAND ----------

-- DELETE FROM WAREHOUSE.DimBenefits
-- WHERE BenefitsId = 1791

-- UPDATE WAREHOUSE.DimBenefits
-- SET Benefits0 = 'This is an empty Benefit field.'
-- WHERE BenefitsId = 1707

-- COMMAND ----------

SELECT * FROM WAREHOUSE.DimBenefits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimHiringProcess

-- COMMAND ----------

-- DBTITLE 1,DimHiringProcess Table Definition
CREATE TABLE WAREHOUSE.DimHiringProcess (
  HiringProcessKey     BIGINT GENERATED ALWAYS AS IDENTITY,
  HiringProcessId      BIGINT NOT NULL,
  SourceSystem         STRING NOT NULL,
  HiringProcessSteps0  STRING,
  HiringProcessSteps1  STRING,
  HiringProcessSteps2  STRING,
  HiringProcessSteps3  STRING,
  HiringProcessSteps4  STRING,
  HiringProcessSteps5  STRING,
  IngestionDate        TIMESTAMP,
  IsActive             BOOLEAN,
  StartDate            TIMESTAMP,
  EndDate              TIMESTAMP
)
USING DELTA OPTIONS (path '/mnt/adlslirkov/it-job-boards/Warehouse/DimHiringProcess')
TBLPROPERTIES ('external.table.purge'='true')
COMMENT 'The Hiring Process dimension'

-- COMMAND ----------

DROP TABLE WAREHOUSE.DimHiringProcess

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("/mnt/adlslirkov/it-job-boards/Warehouse/DimHiringProcess", True)

-- COMMAND ----------

-- DELETE FROM WAREHOUSE.DimHiringProcess
-- WHERE HiringProcessId = 1794

-- UPDATE WAREHOUSE.DimHiringProcess
-- SET HiringProcessSteps0 = 'CV interview'
-- WHERE HiringProcessId = 1245

-- COMMAND ----------

SELECT * FROM WAREHOUSE.DimHiringProcess

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimPerks

-- COMMAND ----------

-- DBTITLE 1,DimPerks Table Definition
CREATE TABLE WAREHOUSE.DimPerks (
  PerksKey       BIGINT GENERATED ALWAYS AS IDENTITY,
  PerksId        BIGINT NOT NULL,
  SourceSystem   STRING NOT NULL,
  Perks0         STRING,
  Perks1         STRING,
  Perks2         STRING,
  Perks3         STRING,
  Perks4         STRING,
  Perks5         STRING,
  Perks6         STRING,
  Perks7         STRING,
  Perks8         STRING,
  Perks9         STRING,
  Perks10        STRING,
  Perks11        STRING,
  Perks12        STRING,
  Perks13        STRING,
  Perks14        STRING,
  IngestionDate  TIMESTAMP,
  IsActive       BOOLEAN,
  StartDate      TIMESTAMP,
  EndDate        TIMESTAMP
)
USING DELTA OPTIONS (path '/mnt/adlslirkov/it-job-boards/Warehouse/DimPerks')
TBLPROPERTIES ('external.table.purge'='true')
COMMENT 'The Perks dimension'

-- COMMAND ----------

DROP TABLE WAREHOUSE.DimPerks

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("/mnt/adlslirkov/it-job-boards/Warehouse/DimPerks", True)

-- COMMAND ----------

DELETE FROM WAREHOUSE.DimPerks
WHERE PerksId = 4

-- UPDATE WAREHOUSE.DimPerks
-- SET Perks0 = 'Play games'
-- WHERE PerksId = 20

-- COMMAND ----------

SELECT * FROM WAREHOUSE.DimPerks

-- COMMAND ----------


