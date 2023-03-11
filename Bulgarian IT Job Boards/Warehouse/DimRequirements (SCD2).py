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
# MAGIC ## Create DimRequirements

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data

# COMMAND ----------

# Read base
df_requirements_noblehire_base = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Noblehire.io/base/jobRequirements/{current_year}/{current_month}/{current_day}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data

# COMMAND ----------

# Select and rename columns
df_requirements_noblehire_enriched = (
    df_requirements_noblehire_base
    .select(
        col("id").alias("RequirementsId"),
        col("Source").alias("SourceSystem"),
        col("requirements_0_title").alias("Requirements0"),
        col("requirements_1_title").alias("Requirements1"),
        col("requirements_2_title").alias("Requirements2"),
        col("requirements_3_title").alias("Requirements3"),
        col("requirements_4_title").alias("Requirements4"),
        col("requirements_5_title").alias("Requirements5"),
        col("requirements_6_title").alias("Requirements6"),
        col("requirements_7_title").alias("Requirements7"),
        col("requirements_8_title").alias("Requirements8"),
        col("requirements_9_title").alias("Requirements9"),
        col("requirements_10_title").alias("Requirements10"),
        col("requirements_11_title").alias("Requirements11"),
        col("requirements_12_title").alias("Requirements12"),
        col("requirements_13_title").alias("Requirements13"),
        col("requirements_14_title").alias("Requirements14"), 
        col("requirements_15_title").alias("Requirements15"),
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
df_requirements_noblehire_enriched = (
    df_requirements_noblehire_enriched
    .withColumn("IsActive", lit(True))
    .withColumn("StartDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("EndDate", lit(None).cast(StringType()))
)

# COMMAND ----------

# DBTITLE 1,Populate Delta Table, if empty
df_requirements_noblehire_enriched.createOrReplaceTempView("Temp_DimRequirements")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO WAREHOUSE.DimRequirements (  
# MAGIC   RequirementsId,    
# MAGIC   SourceSystem,       
# MAGIC   Requirements0,
# MAGIC   Requirements1,
# MAGIC   Requirements2,
# MAGIC   Requirements3,
# MAGIC   Requirements4,
# MAGIC   Requirements5,
# MAGIC   Requirements6,
# MAGIC   Requirements7,
# MAGIC   Requirements8,
# MAGIC   Requirements9,
# MAGIC   Requirements10,
# MAGIC   Requirements11,
# MAGIC   Requirements12,
# MAGIC   Requirements13,
# MAGIC   Requirements14,
# MAGIC   Requirements15,
# MAGIC   IngestionDate,      
# MAGIC   IsActive,           
# MAGIC   StartDate,          
# MAGIC   EndDate            
# MAGIC )
# MAGIC SELECT * FROM Temp_DimRequirements

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 Logic

# COMMAND ----------

# DBTITLE 1,Create Temp Staging Table
# Create the Source Data Frame
sourceDF = df_requirements_noblehire_enriched
sourceDF.display()
print("Count: {}".format(sourceDF.count()))

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
deltaJobRequirements = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimRequirements")

targetDF = deltaJobRequirements.toDF()
targetDF.display()

# COMMAND ----------

# DBTITLE 1,Join source and target
targetDF = targetDF.filter(col("IsActive") == True).select(*[c for c in targetDF.columns if c not in ["IsActive", "StartDate", "EndDate"]])


joinDF = (
    sourceDF
    .join(
        targetDF, 
        (sourceDF.RequirementsId == targetDF.RequirementsId),
#         & (targetDF.IsActive == "true"),
        "outer"
    )
    .select(
        sourceDF["*"],
        targetDF.RequirementsId.alias("target_RequirementsId"),
        targetDF.SourceSystem.alias("target_SourceSystem"),
        targetDF.Requirements0.alias("target_Requirements0"),
        targetDF.Requirements1.alias("target_Requirements1"),
        targetDF.Requirements2.alias("target_Requirements2"),
        targetDF.Requirements3.alias("target_Requirements3"),
        targetDF.Requirements4.alias("target_Requirements4"),
        targetDF.Requirements5.alias("target_Requirements5"),
        targetDF.Requirements6.alias("target_Requirements6"),
        targetDF.Requirements7.alias("target_Requirements7"),
        targetDF.Requirements8.alias("target_Requirements8"),
        targetDF.Requirements9.alias("target_Requirements9"),
        targetDF.Requirements10.alias("target_Requirements10"),
        targetDF.Requirements11.alias("target_Requirements11"),
        targetDF.Requirements12.alias("target_Requirements12"),
        targetDF.Requirements13.alias("target_Requirements13"),
        targetDF.Requirements14.alias("target_Requirements14"),
        targetDF.Requirements15.alias("target_Requirements15"),
        targetDF.IngestionDate.alias("target_IngestionDate")
    )
)

joinDF.display()

# COMMAND ----------

# DBTITLE 1,Hash source and target columns and compare them
filterDF = joinDF.filter(xxhash64(*[c for c in joinDF.columns if c.startswith("target") == False and "IngestionDate" not in c]) != xxhash64(*[c for c in joinDF.columns if c.startswith("target") == True and "IngestionDate" not in c])).withColumn("MergeKey", col("target_RequirementsId"))

filterDF.display()

# COMMAND ----------

# DBTITLE 1,Add MergeKey and set it to null where Id is not null
dummyDF = filterDF.filter(col("target_RequirementsId").isNotNull()).withColumn("MergeKey", lit(None))

dummyDF.display()

# COMMAND ----------

# DBTITLE 1,Union DFs
scdDF = filterDF.union(dummyDF)

scdDF.display()

# COMMAND ----------

# DBTITLE 1,Merge
(deltaJobRequirements.alias("target")
 .merge(
     scdDF.alias("source"),
     "target.RequirementsId = source.MergeKey"
 )
 .whenMatchedUpdate(set = 
    {
        "SourceSystem": "'Noblehire.io'",
        "IsActive": "'False'", 
        "EndDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
    }
 )
 .whenNotMatchedInsert(
     condition = "source.RequirementsId IS NOT NULL",
     values =
     {
        "RequirementsId": "source.RequirementsId",
        "SourceSystem": "source.SourceSystem",
        "Requirements0": "source.Requirements0",
        "Requirements1": "source.Requirements1",
        "Requirements2": "source.Requirements2",
        "Requirements3": "source.Requirements3",
        "Requirements4": "source.Requirements4",
        "Requirements5": "source.Requirements5",
        "Requirements6": "source.Requirements6",
        "Requirements7": "source.Requirements7",
        "Requirements8": "source.Requirements8",
        "Requirements9": "source.Requirements9",
        "Requirements10": "source.Requirements10",
        "Requirements11": "source.Requirements11",
        "Requirements12": "source.Requirements12",
        "Requirements13": "source.Requirements13",
        "Requirements14": "source.Requirements14",
        "Requirements15": "source.Requirements15",
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
deltaJobRequirements.history().display()

# COMMAND ----------

# DBTITLE 1,Compare Delta Table records with records in the Source DataFrame
# Read delta table into DataFrame
deltaFinal = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimRequirements")
finalTargetDF = deltaFinal.toDF()

# Raise error if there are records in the delta table (when filtered to show only active records), which do not exists in the source DataFrame
targetExceptSourceCount = finalTargetDF.where(col("IsActive") == True).select("RequirementsId").exceptAll(sourceDF.select("RequirementsId")).count()
targetEqualsSourceCount = finalTargetDF.where(col("IsActive") == True).count() == sourceDF.count()

if targetExceptSourceCount > 0 or targetEqualsSourceCount == False:
    raise Exception("There are records in source, which do not exist in target.")
