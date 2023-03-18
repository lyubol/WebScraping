-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## DimActivities

-- COMMAND ----------

-- DBTITLE 1,DimActivities View Definition
CREATE VIEW  WAREHOUSE.Current_DimActivities
AS 
SELECT ActivitiesKey,
       ActivitiesId,
       SourceSystem,
       ActivitiesTimePercent0,
       Activities0,
       ActivitiesTimePercent1,
       Activities1,
       ActivitiesTimePercent2,
       Activities2,
       ActivitiesTimePercent3,
       Activities3,
       ActivitiesTimePercent4,
       Activities4,
       ActivitiesTimePercent5,
       Activities5,
       ActivitiesTimePercent6,
       Activities6,
       IngestionDate,
       IsActive,
       StartDate,
       EndDate
FROM   WAREHOUSE.DimActivities
WHERE  IsActive = True

-- COMMAND ----------

SELECT * FROM WAREHOUSE.Current_DimActivities

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimAwards

-- COMMAND ----------

-- DBTITLE 1,DimAwards View Definition
CREATE VIEW  WAREHOUSE.Current_DimAwards
AS 
SELECT AwardsKey,
       AwardsId,
       SourceSystem,
       Awards0,
       Awards1,
       Awards2,
       Awards3,
       Awards4,
       Awards5,
       Awards6,
       Awards7,
       Awards8,
       IngestionDate,
       IsActive,
       StartDate,
       EndDate
FROM   WAREHOUSE.DimAwards
WHERE  IsActive = True

-- COMMAND ----------

SELECT * FROM WAREHOUSE.Current_DimAwards

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimBenefits

-- COMMAND ----------

-- DBTITLE 1,DimBenefits View Definition
CREATE VIEW  WAREHOUSE.Current_DimBenefits
AS 
SELECT BenefitsKey,
       BenefitsId,
       SourceSystem,
       Benefits0,
       Benefits1,
       Benefits2,
       Benefits3,
       Benefits4,
       Benefits5,
       Benefits6,
       Benefits7,
       Benefits8,
       Benefits9,
       Benefits10,
       Benefits11,
       Benefits12,
       Benefits13,
       Benefits14,
       Benefits15,
       Benefits16,
       Benefits17,
       Benefits18,
       IngestionDate,
       IsActive,
       StartDate,
       EndDate
FROM   WAREHOUSE.DimBenefits
WHERE  IsActive = True

-- COMMAND ----------

SELECT * FROM WAREHOUSE.Current_DimBenefits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimHiringProcess

-- COMMAND ----------

-- DBTITLE 1,DimHiringProcess View Definition
CREATE VIEW  WAREHOUSE.Current_DimHiringProcess
AS 
SELECT HiringProcessKey,
       HiringProcessId,
       SourceSystem,
       HiringProcessSteps0,
       HiringProcessSteps1,
       HiringProcessSteps2,
       HiringProcessSteps3,
       HiringProcessSteps4,
       HiringProcessSteps5,
       IngestionDate,
       IsActive,
       StartDate,
       EndDate
FROM   WAREHOUSE.DimHiringProcess
WHERE  IsActive = True

-- COMMAND ----------

SELECT * FROM WAREHOUSE.Current_DimHiringProcess

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimPerks

-- COMMAND ----------

-- DBTITLE 1,DimPerks View Definition
CREATE VIEW  WAREHOUSE.Current_DimPerks
AS 
SELECT PerksKey,
       PerksId,
       SourceSystem,
       Perks0,
       Perks1,
       Perks2,
       Perks3,
       Perks4,
       Perks5,
       Perks6,
       Perks7,
       Perks8,
       Perks9,
       Perks10,
       Perks11,
       Perks12,
       Perks13,
       Perks14,
       IngestionDate,
       IsActive,
       StartDate,
       EndDate
FROM   WAREHOUSE.DimPerks
WHERE  IsActive = True

-- COMMAND ----------

SELECT * FROM WAREHOUSE.Current_DimPerks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimRequirements

-- COMMAND ----------

-- DBTITLE 1,DimRequirements View Definition
CREATE VIEW  WAREHOUSE.Current_DimRequirements
AS 
SELECT RequirementsKey,
       RequirementsId,
       SourceSystem,
       Requirements0,
       Requirements1,
       Requirements2,
       Requirements3,
       Requirements4,
       Requirements5,
       Requirements6,
       Requirements7,
       Requirements8,
       Requirements9,
       Requirements10,
       Requirements11,
       Requirements12,
       Requirements13,
       Requirements14,
       Requirements15,
       IngestionDate,
       IsActive,
       StartDate,
       EndDate
FROM   WAREHOUSE.DimRequirements
WHERE  IsActive = True

-- COMMAND ----------

SELECT * FROM WAREHOUSE.Current_DimRequirements

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimResponsibilities

-- COMMAND ----------

-- DBTITLE 1,DimResponsibilities View Definition
CREATE VIEW  WAREHOUSE.Current_DimResponsibilities
AS 
SELECT ResponsibilitiesKey,
       ResponsibilitiesId,
       SourceSystem,
       Responsibilities0,
       Responsibilities1,
       Responsibilities2,
       Responsibilities3,
       Responsibilities4,
       Responsibilities5,
       Responsibilities6,
       Responsibilities7,
       Responsibilities8,
       Responsibilities9,
       Responsibilities10,
       Responsibilities11,
       Responsibilities12,
       Responsibilities13,
       Responsibilities14,
       Responsibilities15,
       IngestionDate,
       IsActive,
       StartDate,
       EndDate
FROM   WAREHOUSE.DimResponsibilities
WHERE  IsActive = True

-- COMMAND ----------

SELECT * FROM WAREHOUSE.Current_DimResponsibilities

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimTools

-- COMMAND ----------

-- DBTITLE 1,DimTools View Definition
CREATE VIEW  WAREHOUSE.Current_DimTools
AS 
SELECT ToolsKey,
       ToolsId,
       SourceSystem,
       Tools0,
       Tools1,
       Tools2,
       Tools3,
       Tools4,
       Tools5,
       Tools6,
       Tools7,
       Tools8,
       Tools9,
       Tools10,
       Tools11,
       Tools12,
       Tools13,
       Tools14,
       Tools15,
       IngestionDate,
       IsActive,
       StartDate,
       EndDate
FROM   WAREHOUSE.DimTools
WHERE  IsActive = True

-- COMMAND ----------

SELECT * FROM WAREHOUSE.Current_DimTools

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimValues

-- COMMAND ----------

-- DBTITLE 1,DimValues View Definition
CREATE VIEW  WAREHOUSE.Current_DimValues
AS 
SELECT ValuesKey,
       ValuesId,
       SourceSystem,
       ValuesTitle0,
       ValuesText0,
       ValuesTitle1,
       ValuesText1,
       ValuesTitle2,
       ValuesText2,
       ValuesTitle3,
       ValuesText3,
       ValuesTitle4,
       ValuesText4,
       ValuesTitle5,
       ValuesText5,
       ValuesTitle6,
       ValuesText6,
       ValuesTitle7,
       ValuesText7,
       IngestionDate,
       IsActive,
       StartDate,
       EndDate
FROM   WAREHOUSE.DimValues
WHERE  IsActive = True

-- COMMAND ----------

SELECT * FROM WAREHOUSE.Current_DimValues

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimCompany

-- COMMAND ----------

-- DBTITLE 1,DimCompany View Definition
CREATE VIEW  WAREHOUSE.Current_DimCompany
AS 
SELECT CompanyKey,
       CompanyId,
       SourceSystem,
       CompanyName,
       Overview,
       Product,
       IsPublic,
       CompanySlug,
       IngestionDate,
       IsActive,
       StartDate,
       EndDate
FROM   WAREHOUSE.DimCompany
WHERE  IsActive = True

-- COMMAND ----------

SELECT * FROM WAREHOUSE.Current_DimCompany

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimLocations

-- COMMAND ----------

-- DBTITLE 1,DimLocations View Definition
CREATE VIEW  WAREHOUSE.Current_DimLocations
AS 
SELECT LocationKey, 
       LocationId,
       CompanyId,
       SourceSystem,
       LocationComment0,
       LocationFounded0,
       LocationTeamSize0,
       LocationAddress0,
       Latitude0,
       Longitude0,
       LocationComment1,
       LocationFounded1,
       LocationTeamSize1,
       LocationAddress1,
       Latitude1,
       Longitude1,
       LocationComment2,
       LocationFounded2,
       LocationTeamSize2,
       LocationAddress2,
       Latitude2,
       Longitude2,
       LocationComment3,
       LocationFounded3,
       LocationTeamSize3,
       LocationAddress3,
       Latitude3,
       Longitude3,
       IngestionDate,
       IsActive,
       StartDate,
       EndDate
FROM   WAREHOUSE.DimLocations
WHERE  IsActive = True

-- COMMAND ----------

SELECT * FROM WAREHOUSE.Current_DimLocations

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimJunk

-- COMMAND ----------

-- DBTITLE 1,DimJunk View Definition
CREATE VIEW  WAREHOUSE.Current_DimJunk
AS 
SELECT JunkKey, 
       JunkId,
       SourceSystem,
       BusinessTraveling,
       CustomerFacing,
       Description,
       FullyRemote,
       HomeOfficeDays,
       HomeOfficePer,
       JobType,
       MainDatabase,
       OfferingStock,
       PrimaryLanguage,
       ProductDescription,
       Role,
       SalaryCurrency,
       SalaryMax,
       SalaryMin,
       SalaryPeriod,
       SecondaryLanguage,
       SecondaryPlatform,
       Seniority,
       Slug,
       TeamLeadName,
       TeamLeadRole,
       TeamSizeMax,
       TeamSizeMin,
       Title,
       IngestionDate,
       IsActive,
       StartDate,
       EndDate
FROM   WAREHOUSE.DimJunk
WHERE  IsActive = True

-- COMMAND ----------

SELECT * FROM WAREHOUSE.Current_DimJunk

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## FctPosts

-- COMMAND ----------

-- DBTITLE 1,FctPosts View Definition
CREATE VIEW  WAREHOUSE.Current_FctPosts
AS 
SELECT JobPostKey,
       JobPostId,
       DatePosted,
       SourceSystem,
       ActivitiesId,
       AwardsId,
       BenefitsId,
       CompanyId,
       HiringProcessId,
       LocationId,
       PerksId,
       RequirementsId,
       ResponsibilitiesId,
       ToolsId,
       ValuesId,
       JunkId
FROM   WAREHOUSE.FctPosts

-- COMMAND ----------

SELECT * FROM WAREHOUSE.Current_FctPosts
