-- Databricks notebook source
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

SELECT * FROM WAREHOUSE.DimBenefits

-- COMMAND ----------

-- DELETE FROM WAREHOUSE.DimBenefits
-- WHERE BenefitsId = 1791

-- UPDATE WAREHOUSE.DimBenefits
-- SET Benefits0 = 'This is an empty Benefit field.'
-- WHERE BenefitsId = 1707

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

SELECT * FROM WAREHOUSE.DimHiringProcess

-- COMMAND ----------

-- DELETE FROM WAREHOUSE.DimHiringProcess
-- WHERE HiringProcessId = 1794

-- UPDATE WAREHOUSE.DimHiringProcess
-- SET HiringProcessSteps0 = 'CV interview'
-- WHERE HiringProcessId = 1245

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

SELECT * FROM WAREHOUSE.DimPerks

-- COMMAND ----------

-- DELETE FROM WAREHOUSE.DimPerks
-- WHERE PerksId = 4

-- UPDATE WAREHOUSE.DimPerks
-- SET Perks0 = 'Play games'
-- WHERE PerksId = 20

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

SELECT * FROM WAREHOUSE.DimRequirements

-- COMMAND ----------

-- DELETE FROM WAREHOUSE.DimRequirements
-- WHERE RequirementsId = 32

-- UPDATE WAREHOUSE.DimRequirements
-- SET Requirements0 = 'Test'
-- WHERE RequirementsId = 31

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

SELECT * FROM WAREHOUSE.DimResponsibilities

-- COMMAND ----------

-- DELETE FROM WAREHOUSE.DimResponsibilities
-- WHERE ResponsibilitiesId = 32

-- UPDATE WAREHOUSE.DimResponsibilities
-- SET Responsibilities0 = 'Test'
-- WHERE ResponsibilitiesId = 31

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

SELECT * FROM WAREHOUSE.DimTools

-- COMMAND ----------

-- DELETE FROM WAREHOUSE.DimTools
-- WHERE ToolsId = 32

-- UPDATE WAREHOUSE.DimTools
-- SET Tools0 = 'Test'
-- WHERE ToolsId = 31

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

SELECT * FROM WAREHOUSE.DimValues

-- COMMAND ----------

-- DELETE FROM WAREHOUSE.DimValues
-- WHERE ValuesId = 4

-- UPDATE WAREHOUSE.DimValues
-- SET ValuesTitle0 = 'Test'
-- WHERE ValuesId = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimCompany

-- COMMAND ----------

-- DBTITLE 1,DimCompany Table Definition
CREATE TABLE WAREHOUSE.DimCompany  (
  CompanyKey     BIGINT GENERATED ALWAYS AS IDENTITY,
  CompanyId      BIGINT NOT NULL,
  SourceSystem   STRING NOT NULL,
  CompanyName    STRING,
  Overview       STRING,
  Product        STRING,
  IsPublic       BOOLEAN,
  CompanySlug    STRING,
  IngestionDate  TIMESTAMP,
  IsActive       BOOLEAN,
  StartDate      TIMESTAMP,
  EndDate        TIMESTAMP
)
USING DELTA OPTIONS (path '/mnt/adlslirkov/it-job-boards/Warehouse/DimCompany')
TBLPROPERTIES ('external.table.purge'='true')
COMMENT 'The Company dimension'

-- COMMAND ----------

DROP TABLE WAREHOUSE.DimCompany

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("/mnt/adlslirkov/it-job-boards/Warehouse/DimCompany", True)

-- COMMAND ----------

SELECT * FROM WAREHOUSE.DimCompany

-- COMMAND ----------

-- DELETE FROM WAREHOUSE.DimCompany
-- WHERE CompanyId = 4

-- UPDATE WAREHOUSE.DimCompany
-- SET CompanyName = 'TestChain'
-- WHERE CompanyId = 9

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimLocations

-- COMMAND ----------

-- DBTITLE 1,DimLocations Table Definition
CREATE TABLE WAREHOUSE.DimLocations  (
  LocationKey       BIGINT GENERATED ALWAYS AS IDENTITY, 
  LocationId        BIGINT NOT NULL,
  CompanyId         BIGINT,
  SourceSystem      STRING,
  LocationComment0  STRING,
  LocationFounded0  INT,
  LocationTeamSize0 STRING,
  LocationAddress0  STRING,
  Latitude0         STRING,
  Longitude0        STRING,
  LocationComment1  STRING,
  LocationFounded1  INT,
  LocationTeamSize1 STRING,
  LocationAddress1  STRING,
  Latitude1         STRING,
  Longitude1        STRING,
  LocationComment2  STRING,
  LocationFounded2  INT,
  LocationTeamSize2 STRING,
  LocationAddress2  STRING,
  Latitude2         STRING,
  Longitude2        STRING,
  LocationComment3  STRING,
  LocationFounded3  INT,
  LocationTeamSize3 STRING,
  LocationAddress3  STRING,
  Latitude3         STRING,
  Longitude3        STRING,
  IngestionDate     TIMESTAMP,
  IsActive          BOOLEAN,
  StartDate         TIMESTAMP,
  EndDate           TIMESTAMP
)
USING DELTA OPTIONS (path '/mnt/adlslirkov/it-job-boards/Warehouse/DimLocations')
TBLPROPERTIES ('external.table.purge'='true')
COMMENT 'The Locations dimension'

-- COMMAND ----------

DROP TABLE WAREHOUSE.DimLocations

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("/mnt/adlslirkov/it-job-boards/Warehouse/DimLocations", True)

-- COMMAND ----------

SELECT * FROM WAREHOUSE.DimLocations

-- COMMAND ----------

-- DELETE FROM WAREHOUSE.DimLocations
-- WHERE LocationId = 32

-- UPDATE WAREHOUSE.DimLocations
-- SET LocationFounded0 = 2023
-- WHERE LocationId = 31

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimJunk

-- COMMAND ----------

-- DBTITLE 1,DimJunk Table Definition
CREATE TABLE WAREHOUSE.DimJunk (
  JunkKey            BIGINT GENERATED ALWAYS AS IDENTITY, 
  JunkId             BIGINT NOT NULL,
  SourceSystem       STRING,
  BusinessTraveling  BOOLEAN,
  CustomerFacing     BOOLEAN,
  Description        STRING,
  FullyRemote        BOOLEAN,
  HomeOfficeDays     INT,
  HomeOfficePer      STRING,
  JobType            STRING,
  MainDatabase       STRING,
  OfferingStock      BOOLEAN,
  PrimaryLanguage    STRING,
  ProductDescription STRING,
  Role               STRING,
  SalaryCurrency     STRING,
  SalaryMax          INT,
  SalaryMin          INT,
  SalaryPeriod       STRING,
  SecondaryLanguage  STRING,
  SecondaryPlatform  STRING,
  Seniority          STRING,
  Slug               STRING,
  TeamLeadName       STRING,
  TeamLeadRole       STRING,
  TeamSizeMax        INT,
  TeamSizeMin        INT,
  Title              STRING,
  IngestionDate      TIMESTAMP,
  IsActive           BOOLEAN,
  StartDate          TIMESTAMP,
  EndDate            TIMESTAMP
)
USING DELTA OPTIONS (path '/mnt/adlslirkov/it-job-boards/Warehouse/DimJunk')
TBLPROPERTIES ('external.table.purge'='true')
COMMENT 'The Junk dimension'

-- COMMAND ----------

DROP TABLE WAREHOUSE.DimJunk

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("/mnt/adlslirkov/it-job-boards/Warehouse/DimJunk", True)

-- COMMAND ----------

SELECT * FROM WAREHOUSE.DimJunk

-- COMMAND ----------

-- DELETE FROM WAREHOUSE.DimJunk
-- WHERE JunkId = 1794

-- UPDATE WAREHOUSE.DimJunk
-- SET BusinessTraveling = true
-- WHERE JunkId = 1741

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## FctPosts

-- COMMAND ----------

-- DBTITLE 1,FctPosts Table Definition
CREATE TABLE WAREHOUSE.FctPosts  (
  JobPostKey         BIGINT GENERATED ALWAYS AS IDENTITY,
  JobPostId          BIGINT NOT NULL,
  DatePosted         INT NOT NULL,
  SourceSystemId     INT NOT NULL,
  ActivitiesId       INT,
  AwardsId           INT,
  BenefitsId         INT,
  CompanyId          INT,
  HiringProcessId    INT,
  LocationId         INT,
  PerksId            INT,
  RequirementsId     INT,
  ResponsibilitiesId INT,
  ToolsId            INT,
  ValuesId           INT,
  JunkId             INT
)
USING DELTA OPTIONS (path '/mnt/adlslirkov/it-job-boards/Warehouse/FctPosts')
TBLPROPERTIES ('external.table.purge'='true')
COMMENT 'The Posts fact'

-- COMMAND ----------

DROP TABLE WAREHOUSE.FctPosts

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("/mnt/adlslirkov/it-job-boards/Warehouse/FctPosts", True)

-- COMMAND ----------

SELECT * FROM WAREHOUSE.FctPosts
