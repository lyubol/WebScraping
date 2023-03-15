# Databricks notebook source
# MAGIC %run "lirkov/IT Job Boards/Main"

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import Window
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Define variables
# Date variables 
current_year = date.today().year
current_month = "0" + str(date.today().month) if len(str(date.today().month)) == 1 else date.today().month
current_day = "0" + str(date.today().day) if len(str(date.today().day)) == 1 else date.today().day

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create DimJunk

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data

# COMMAND ----------

# Read base
df_posts_noblehire_base = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Noblehire.io/base/posts/{current_year}/{current_month}/{current_day}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data

# COMMAND ----------

# Select and rename columns
df_posts_noblehire_enriched = (
    df_posts_noblehire_base
    .select(
        col("id").alias("JunkId"),
        col("Source").alias("SourceSystem"),
        col("businessTraveling").alias("BusinessTraveling"),
        col("customerFacing").alias("CustomerFacing"),
        col("description").alias("Description"),
        col("fullyRemote").alias("FullyRemote"),
        col("homeOfficeDays").alias("HomeOfficeDays").cast("int"),
        col("homeOfficePer").alias("HomeOfficePer"),
        col("jobType").alias("JobType"),
        col("mainDatabase").alias("MainDatabase"),
        col("offeringStock").alias("OfferingStock"),
        col("primaryLanguage").alias("PrimaryLanguage"),
        col("productDescription").alias("ProductDescription"),
        col("role").alias("Role"),
        col("salaryCurrency").alias("SalaryCurrency"),
        col("salaryMax").alias("SalaryMax").cast("int"),
        col("salaryMin").alias("SalaryMin").cast("int"),
        col("salaryPeriod").alias("SalaryPeriod"),
        col("secondaryLanguage").alias("SecondaryLanguage"),
        col("secondaryPlatform").alias("SecondaryPlatform"),
        col("seniority").alias("Seniority"),
        col("slug").alias("Slug"),
        col("teamLeadName").alias("TeamLeadName"),
        col("teamLeadRole").alias("TeamLeadRole"),
        col("teamSizeMax").alias("TeamSizeMax").cast("int"),
        col("teamSizeMin").alias("TeamSizeMin").cast("int"),
        col("title").alias("Title"),
        col("IngestionDate")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 Setup

# COMMAND ----------

# MAGIC %md
# MAGIC The following commands are used for the initial setup of the delta table

# COMMAND ----------

# DBTITLE 1,Add SCD Type 2 Columns to Delta Table
# df_posts_noblehire_enriched = (
#     df_posts_noblehire_enriched
#     .withColumn("IsActive", lit(True))
#     .withColumn("StartDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
#     .withColumn("EndDate", lit(None).cast(StringType()))
# )

# COMMAND ----------

# DBTITLE 1,Populate Delta Table, if empty
# df_posts_noblehire_enriched.createOrReplaceTempView("Temp_DimJunk")

# COMMAND ----------

# %sql

# INSERT INTO WAREHOUSE.DimJunk (      
#   JunkId,           
#   SourceSystem,     
#   BusinessTraveling,
#   CustomerFacing,   
#   Description,      
#   FullyRemote,      
#   HomeOfficeDays,   
#   HomeOfficePer,    
#   JobType,          
#   MainDatabase,     
#   OfferingStock,    
#   PrimaryLanguage,  
#   ProductDescription,
#   Role,            
#   SalaryCurrency,   
#   SalaryMax,        
#   SalaryMin,        
#   SalaryPeriod,     
#   SecondaryLanguage,
#   SecondaryPlatform,
#   Seniority,        
#   Slug,             
#   TeamLeadName,     
#   TeamLeadRole,     
#   TeamSizeMax,      
#   TeamSizeMin,      
#   Title,
#   IngestionDate,      
#   IsActive,           
#   StartDate,          
#   EndDate            
# )
# SELECT * FROM Temp_DimJunk

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 Logic

# COMMAND ----------

# DBTITLE 1,Create Temp Staging Table
# Create the Source Data Frame
sourceDF = df_posts_noblehire_enriched
sourceDF.display()
print("Count: {}".format(sourceDF.count()))

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
deltaJunk = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimJunk")

targetDF = deltaJunk.toDF()
targetDF.display()

# COMMAND ----------

# DBTITLE 1,Join source and target
targetDF = targetDF.filter(col("IsActive") == True).select(*[c for c in targetDF.columns if c not in ["IsActive", "StartDate", "EndDate"]])


joinDF = (
    sourceDF
    .join(
        targetDF, 
        (sourceDF.JunkId == targetDF.JunkId),
#         & (targetDF.IsActive == "true"),
        "outer"
    )
    .select(
        sourceDF["*"],
        targetDF.JunkId.alias("target_JunkId"),
        targetDF.SourceSystem.alias("target_SourceSystem"),
        targetDF.BusinessTraveling.alias("target_BusinessTraveling"),
        targetDF.CustomerFacing.alias("target_CustomerFacing"),
        targetDF.Description.alias("target_Description"),
        targetDF.FullyRemote.alias("target_FullyRemote"),
        targetDF.HomeOfficeDays.alias("target_HomeOfficeDays"),
        targetDF.HomeOfficePer.alias("target_HomeOfficePer"),
        targetDF.JobType.alias("target_JobType"),
        targetDF.MainDatabase.alias("target_MainDatabase"),
        targetDF.OfferingStock.alias("target_OfferingStock"),
        targetDF.PrimaryLanguage.alias("target_PrimaryLanguage"),
        targetDF.ProductDescription.alias("target_ProductDescription"),
        targetDF.Role.alias("target_Role"),
        targetDF.SalaryCurrency.alias("target_SalaryCurrency"),
        targetDF.SalaryMax.alias("target_SalaryMax"),
        targetDF.SalaryMin.alias("target_SalaryMin"),
        targetDF.SalaryPeriod.alias("target_SalaryPeriod"),
        targetDF.SecondaryLanguage.alias("target_SecondaryLanguage"),
        targetDF.SecondaryPlatform.alias("target_SecondaryPlatform"),
        targetDF.Seniority.alias("target_Seniority"),
        targetDF.Slug.alias("target_Slug"),
        targetDF.TeamLeadName.alias("target_TeamLeadName"),
        targetDF.TeamLeadRole.alias("target_TeamLeadRole"),
        targetDF.TeamSizeMax.alias("target_TeamSizeMax"),
        targetDF.TeamSizeMin.alias("target_TeamSizeMin"),
        targetDF.Title.alias("target_Title"),
        targetDF.IngestionDate.alias("target_IngestionDate")
    )
)

joinDF.display()

# COMMAND ----------

# DBTITLE 1,Hash source and target columns and compare them
filterDF = joinDF.filter(xxhash64(*[c for c in joinDF.columns if c.startswith("target") == False and "IngestionDate" not in c]) != xxhash64(*[c for c in joinDF.columns if c.startswith("target") == True and "IngestionDate" not in c])).withColumn("MergeKey", col("target_JunkId"))

filterDF.display()

# COMMAND ----------

# DBTITLE 1,Add MergeKey and set it to null where Id is not null
dummyDF = filterDF.filter(col("target_JunkId").isNotNull()).withColumn("MergeKey", lit(None))

dummyDF.display()

# COMMAND ----------

# DBTITLE 1,Union DFs
scdDF = filterDF.union(dummyDF)

scdDF.display()

# COMMAND ----------

# DBTITLE 1,Merge
(deltaJunk.alias("target")
 .merge(
     scdDF.alias("source"),
     "target.JunkId = source.MergeKey"
 )
 .whenMatchedUpdate(set = 
    {
        "SourceSystem": "'Noblehire.io'",
        "IsActive": "'False'", 
        "EndDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
    }
 )
 .whenNotMatchedInsert(
     condition = "source.JunkId IS NOT NULL",
     values =
     {
        "JunkId": "source.JunkId",
        "SourceSystem": "source.SourceSystem",
        "BusinessTraveling": "source.BusinessTraveling",
        "CustomerFacing": "source.CustomerFacing",
        "Description": "source.Description",
        "FullyRemote": "source.FullyRemote",
        "HomeOfficeDays": "source.HomeOfficeDays",
        "HomeOfficePer": "source.HomeOfficePer",
        "JobType": "source.JobType",
        "MainDatabase": "source.MainDatabase",
        "OfferingStock": "source.OfferingStock",
        "PrimaryLanguage": "source.PrimaryLanguage",
        "ProductDescription": "source.ProductDescription",
        "Role": "source.Role",
        "SalaryCurrency": "source.SalaryCurrency",
        "SalaryMax": "source.SalaryMax",
        "SalaryMin": "source.SalaryMin",
        "SalaryPeriod": "source.SalaryPeriod",
        "SecondaryLanguage": "source.SecondaryLanguage",
        "SecondaryPlatform": "source.SecondaryPlatform",
        "Seniority": "source.Seniority",
        "Slug": "source.Slug",
        "TeamLeadName": "source.TeamLeadName",
        "TeamLeadRole": "source.TeamLeadRole",
        "TeamSizeMax": "source.TeamSizeMax",
        "TeamSizeMin": "source.TeamSizeMin",
        "Title": "source.Title",
        "IngestionDate": "source.IngestionDate",
        "IsActive": "'True'",
        "StartDate": "current_timestamp",
        "EndDate": """to_date('9999-12-31 00:00:00.0000', 'MM-dd-yyyy HH:mm:ss')"""
     }
 )
 .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# DBTITLE 1,Check Delta Table History
deltaJunk.history().display()

# COMMAND ----------

# DBTITLE 1,Compare Delta Table records with records in the Source DataFrame
# Read delta table into DataFrame
deltaFinal = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimJunk")
finalTargetDF = deltaFinal.toDF()

# Raise error if there are records in the delta table (when filtered to show only active records), which do not exists in the source DataFrame
targetExceptSourceCount = finalTargetDF.where(col("IsActive") == True).select("JunkId").exceptAll(sourceDF.select("JunkId")).count()
targetEqualsSourceCount = finalTargetDF.where(col("IsActive") == True).count() == sourceDF.count()

if targetExceptSourceCount > 0 or targetEqualsSourceCount == False:
    raise Exception("There are records in source, which do not exist in target.")
