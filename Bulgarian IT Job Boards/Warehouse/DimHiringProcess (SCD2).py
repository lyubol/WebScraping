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
# MAGIC ## Create DimHiringProcess

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data

# COMMAND ----------

# Read base
df_hiringprocess_noblehire_base = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Noblehire.io/base/jobHiringProcess/{current_year}/{current_month}/{current_day}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data

# COMMAND ----------

# Select and rename columns
df_hiringprocess_noblehire_enriched = (
    df_hiringprocess_noblehire_base
    .select(
        col("id").alias("HiringProcessId"),
        col("Source").alias("SourceSystem"),
        col("hiringProcessSteps_0").alias("HiringProcessSteps0"),
        col("hiringProcessSteps_1").alias("HiringProcessSteps1"),
        col("hiringProcessSteps_2").alias("HiringProcessSteps2"),
        col("hiringProcessSteps_3").alias("HiringProcessSteps3"),
        col("hiringProcessSteps_4").alias("HiringProcessSteps4"),
        col("hiringProcessSteps_5").alias("HiringProcessSteps5"),
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
df_hiringprocess_noblehire_enriched = (
    df_hiringprocess_noblehire_enriched
    .withColumn("IsActive", lit(True))
    .withColumn("StartDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("EndDate", lit(None).cast(StringType()))
)

# COMMAND ----------

# DBTITLE 1,Populate Delta Table, if empty
df_hiringprocess_noblehire_enriched.createOrReplaceTempView("Temp_DimHiringProcess")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO WAREHOUSE.DimHiringProcess (  
# MAGIC   HiringProcessId,    
# MAGIC   SourceSystem,       
# MAGIC   HiringProcessSteps0,
# MAGIC   HiringProcessSteps1,
# MAGIC   HiringProcessSteps2,
# MAGIC   HiringProcessSteps3,
# MAGIC   HiringProcessSteps4,
# MAGIC   HiringProcessSteps5,
# MAGIC   IngestionDate,      
# MAGIC   IsActive,           
# MAGIC   StartDate,          
# MAGIC   EndDate            
# MAGIC )
# MAGIC SELECT * FROM Temp_DimHiringProcess

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 Logic

# COMMAND ----------

# DBTITLE 1,Create Temp Staging Table
# Create the Source Data Frame
sourceDF = df_hiringprocess_noblehire_enriched
sourceDF.display()
print("Count: {}".format(sourceDF.count()))

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
deltaJobHiringProcess = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimHiringProcess")

targetDF = deltaJobHiringProcess.toDF()
targetDF.display()

# COMMAND ----------

# DBTITLE 1,Join source and target
targetDF = targetDF.filter(col("IsActive") == True).select(*[c for c in targetDF.columns if c not in ["IsActive", "StartDate", "EndDate"]])


joinDF = (
    sourceDF
    .join(
        targetDF, 
        (sourceDF.HiringProcessId == targetDF.HiringProcessId),
#         & (targetDF.IsActive == "true"),
        "outer"
    )
    .select(
        sourceDF["*"],
        targetDF.HiringProcessId.alias("target_HiringProcessId"),
        targetDF.SourceSystem.alias("target_SourceSystem"),
        targetDF.HiringProcessSteps0.alias("target_HiringProcessSteps0"),
        targetDF.HiringProcessSteps1.alias("target_HiringProcessSteps1"),
        targetDF.HiringProcessSteps2.alias("target_HiringProcessSteps2"),
        targetDF.HiringProcessSteps3.alias("target_HiringProcessSteps3"),
        targetDF.HiringProcessSteps4.alias("target_HiringProcessSteps4"),
        targetDF.HiringProcessSteps5.alias("target_HiringProcessSteps5"),
        targetDF.IngestionDate.alias("target_IngestionDate")
    )
)

joinDF.display()

# COMMAND ----------

# DBTITLE 1,Hash source and target columns and compare them
filterDF = joinDF.filter(xxhash64(*[c for c in joinDF.columns if c.startswith("target") == False and "IngestionDate" not in c]) != xxhash64(*[c for c in joinDF.columns if c.startswith("target") == True and "IngestionDate" not in c])).withColumn("MergeKey", col("target_HiringProcessId"))

filterDF.display()

# COMMAND ----------

# DBTITLE 1,Add MergeKey and set it to null where Id is not null
dummyDF = filterDF.filter(col("target_HiringProcessId").isNotNull()).withColumn("MergeKey", lit(None))

dummyDF.display()

# COMMAND ----------

# DBTITLE 1,Union DFs
scdDF = filterDF.union(dummyDF)

scdDF.display()

# COMMAND ----------

# DBTITLE 1,Merge
(deltaJobHiringProcess.alias("target")
 .merge(
     scdDF.alias("source"),
     "target.HiringProcessId = source.MergeKey"
 )
 .whenMatchedUpdate(set = 
    {
        "SourceSystem": "'Noblehire.io'",
        "IsActive": "'False'", 
        "EndDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
    }
 )
 .whenNotMatchedInsert(
     condition = "source.HiringProcessId IS NOT NULL",
     values =
     {
        "HiringProcessId": "source.HiringProcessId",
        "SourceSystem": "source.SourceSystem",
        "HiringProcessSteps0": "source.HiringProcessSteps0",
        "HiringProcessSteps1": "source.HiringProcessSteps1",
        "HiringProcessSteps2": "source.HiringProcessSteps2",
        "HiringProcessSteps3": "source.HiringProcessSteps3",
        "HiringProcessSteps4": "source.HiringProcessSteps4",
        "HiringProcessSteps5": "source.HiringProcessSteps5",
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
deltaJobHiringProcess.history().display()

# COMMAND ----------

# DBTITLE 1,Compare Delta Table records with records in the Source DataFrame
# Read delta table into DataFrame
deltaFinal = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimHiringProcess")
finalTargetDF = deltaFinal.toDF()

# Raise error if there are records in the delta table (when filtered to show only active records), which do not exists in the source DataFrame
targetExceptSourceCount = finalTargetDF.where(col("IsActive") == True).select("HiringProcessId").exceptAll(sourceDF.select("HiringProcessId")).count()
targetEqualsSourceCount = finalTargetDF.where(col("IsActive") == True).count() == sourceDF.count()

if targetExceptSourceCount > 0 or targetEqualsSourceCount == False:
    raise Exception("There are records in source, which do not exist in target.")
