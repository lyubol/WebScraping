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

-- DELETE FROM WAREHOUSE.DimPerks
-- WHERE PerksId = 4

-- UPDATE WAREHOUSE.DimPerks
-- SET Perks0 = 'Play games'
-- WHERE PerksId = 20

-- COMMAND ----------

SELECT * FROM WAREHOUSE.DimPerks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimRequirements

-- COMMAND ----------

-- DBTITLE 1,DimRequirements Table Definition
CREATE TABLE WAREHOUSE.DimRequirements  (
  RequirementsKey BIGINT GENERATED ALWAYS AS IDENTITY,
  RequirementsId  BIGINT NOT NULL,
  SourceSystem    STRING NOT NULL,
  Requirements0   STRING,
  Requirements1   STRING,
  Requirements2   STRING,
  Requirements3   STRING,
  Requirements4   STRING,
  Requirements5   STRING,
  Requirements6   STRING,
  Requirements7   STRING,
  Requirements8   STRING,
  Requirements9   STRING,
  Requirements10  STRING,
  Requirements11  STRING,
  Requirements12  STRING,
  Requirements13  STRING,
  Requirements14  STRING,
  Requirements15  STRING,
  IngestionDate   TIMESTAMP,
  IsActive        BOOLEAN,
  StartDate       TIMESTAMP,
  EndDate         TIMESTAMP
)
USING DELTA OPTIONS (path '/mnt/adlslirkov/it-job-boards/Warehouse/DimRequirements')
TBLPROPERTIES ('external.table.purge'='true')
COMMENT 'The Requirements dimension'

-- COMMAND ----------

DROP TABLE WAREHOUSE.DimRequirements

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("/mnt/adlslirkov/it-job-boards/Warehouse/DimRequirements", True)

-- COMMAND ----------

-- DELETE FROM WAREHOUSE.DimRequirements
-- WHERE RequirementsId = 32

-- UPDATE WAREHOUSE.DimRequirements
-- SET Requirements0 = 'Test'
-- WHERE RequirementsId = 31

-- COMMAND ----------

SELECT * FROM WAREHOUSE.DimRequirements

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimResponsibilities

-- COMMAND ----------

-- DBTITLE 1,DimResponsibilities Table Definition
CREATE TABLE WAREHOUSE.DimResponsibilities  (
  ResponsibilitiesKey BIGINT GENERATED ALWAYS AS IDENTITY,
  ResponsibilitiesId  BIGINT NOT NULL,
  SourceSystem        STRING NOT NULL,
  Responsibilities0   STRING,
  Responsibilities1   STRING,
  Responsibilities2   STRING,
  Responsibilities3   STRING,
  Responsibilities4   STRING,
  Responsibilities5   STRING,
  Responsibilities6   STRING,
  Responsibilities7   STRING,
  Responsibilities8   STRING,
  Responsibilities9   STRING,
  Responsibilities10  STRING,
  Responsibilities11  STRING,
  Responsibilities12  STRING,
  Responsibilities13  STRING,
  Responsibilities14  STRING,
  Responsibilities15  STRING,
  IngestionDate       TIMESTAMP,
  IsActive            BOOLEAN,
  StartDate           TIMESTAMP,
  EndDate             TIMESTAMP
)
USING DELTA OPTIONS (path '/mnt/adlslirkov/it-job-boards/Warehouse/DimResponsibilities')
TBLPROPERTIES ('external.table.purge'='true')
COMMENT 'The Responsibilities dimension'

-- COMMAND ----------

DROP TABLE WAREHOUSE.DimResponsibilities

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("/mnt/adlslirkov/it-job-boards/Warehouse/DimResponsibilities", True)

-- COMMAND ----------

-- DELETE FROM WAREHOUSE.DimResponsibilities
-- WHERE ResponsibilitiesId = 32

-- UPDATE WAREHOUSE.DimResponsibilities
-- SET Responsibilities0 = 'Test'
-- WHERE ResponsibilitiesId = 31

-- COMMAND ----------

SELECT * FROM WAREHOUSE.DimResponsibilities

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimTools

-- COMMAND ----------

-- DBTITLE 1,DimTools Table Definition
CREATE TABLE WAREHOUSE.DimTools  (
  ToolsKey       BIGINT GENERATED ALWAYS AS IDENTITY,
  ToolsId        BIGINT NOT NULL,
  SourceSystem   STRING NOT NULL,
  Tools0         STRING,
  Tools1         STRING,
  Tools2         STRING,
  Tools3         STRING,
  Tools4         STRING,
  Tools5         STRING,
  Tools6         STRING,
  Tools7         STRING,
  Tools8         STRING,
  Tools9         STRING,
  Tools10        STRING,
  Tools11        STRING,
  Tools12        STRING,
  Tools13        STRING,
  Tools14        STRING,
  Tools15        STRING,
  IngestionDate  TIMESTAMP,
  IsActive       BOOLEAN,
  StartDate      TIMESTAMP,
  EndDate        TIMESTAMP
)
USING DELTA OPTIONS (path '/mnt/adlslirkov/it-job-boards/Warehouse/DimTools')
TBLPROPERTIES ('external.table.purge'='true')
COMMENT 'The Tools dimension'

-- COMMAND ----------

DROP TABLE WAREHOUSE.DimTools

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("/mnt/adlslirkov/it-job-boards/Warehouse/DimTools", True)

-- COMMAND ----------

-- DELETE FROM WAREHOUSE.DimTools
-- WHERE ToolsId = 32

-- UPDATE WAREHOUSE.DimTools
-- SET Tools0 = 'Test'
-- WHERE ToolsId = 31

-- COMMAND ----------

SELECT * FROM WAREHOUSE.DimTools

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimValues

-- COMMAND ----------

-- DBTITLE 1,DimValues Table Definition
CREATE TABLE WAREHOUSE.DimValues  (
  ValuesKey      BIGINT GENERATED ALWAYS AS IDENTITY,
  ValuesId       BIGINT NOT NULL,
  SourceSystem   STRING NOT NULL,
  ValuesTitle0   STRING,
  ValuesText0    STRING,
  ValuesTitle1   STRING,
  ValuesText1    STRING,
  ValuesTitle2   STRING,
  ValuesText2    STRING,
  ValuesTitle3   STRING,
  ValuesText3    STRING,
  ValuesTitle4   STRING,
  ValuesText4    STRING,
  ValuesTitle5   STRING,
  ValuesText5    STRING,
  ValuesTitle6   STRING,
  ValuesText6    STRING,
  ValuesTitle7   STRING,
  ValuesText7    STRING,
  IngestionDate  TIMESTAMP,
  IsActive       BOOLEAN,
  StartDate      TIMESTAMP,
  EndDate        TIMESTAMP
)
USING DELTA OPTIONS (path '/mnt/adlslirkov/it-job-boards/Warehouse/DimValues')
TBLPROPERTIES ('external.table.purge'='true')
COMMENT 'The Values dimension'

-- COMMAND ----------

DROP TABLE WAREHOUSE.DimValues

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("/mnt/adlslirkov/it-job-boards/Warehouse/DimValues", True)

-- COMMAND ----------

-- DELETE FROM WAREHOUSE.DimValues
-- WHERE ValuesId = 4

-- UPDATE WAREHOUSE.DimValues
-- SET ValuesTitle0 = 'Test'
-- WHERE ValuesId = 1

-- COMMAND ----------

SELECT * FROM WAREHOUSE.DimValues

-- COMMAND ----------


