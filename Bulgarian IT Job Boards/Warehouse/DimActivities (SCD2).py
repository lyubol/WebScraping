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
# MAGIC ## Create DimActivities

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data

# COMMAND ----------

# Read base
df_activities_noblehire_base = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Noblehire.io/base/jobActivities/{current_year}/{current_month}/{current_day}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data

# COMMAND ----------

# Select and rename columns
df_activities_noblehire_enriched = (
    df_activities_noblehire_base
    .select(
        col("id").alias("ActivitiesId"),
        col("Source").alias("SourceSystem"),
        col("activities_0_timePercents").alias("ActivitiesTimePercent0"),
        col("activities_0_title").alias("Activities0"),
        col("activities_1_timePercents").alias("ActivitiesTimePercent1"),
        col("activities_1_title").alias("Activities1"),
        col("activities_2_timePercents").alias("ActivitiesTimePercent2"),
        col("activities_2_title").alias("Activities2"),
        col("activities_3_timePercents").alias("ActivitiesTimePercent3"),
        col("activities_3_title").alias("Activities3"),
        col("activities_4_timePercents").alias("ActivitiesTimePercent4"),
        col("activities_4_title").alias("Activities4"),
        col("activities_5_timePercents").alias("ActivitiesTimePercent5"),
        col("activities_5_title").alias("Activities5"),
        col("activities_6_timePercents").alias("ActivitiesTimePercent6"),
        col("activities_6_title").alias("Activities6"),
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
df_activities_noblehire_enriched = (
    df_activities_noblehire_enriched
    .withColumn("IsActive", lit(True))
    .withColumn("StartDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("EndDate", lit(None).cast(StringType()))
)

# COMMAND ----------

# DBTITLE 1,Populate Delta Table, if empty
df_activities_noblehire_enriched.createOrReplaceTempView("Temp_DimActivities")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO WAREHOUSE.DimActivities (
# MAGIC     ActivitiesId, 
# MAGIC     SourceSystem, 
# MAGIC     ActivitiesTimePercent0, 
# MAGIC     Activities0,
# MAGIC     ActivitiesTimePercent1,
# MAGIC     Activities1,      
# MAGIC     ActivitiesTimePercent2,
# MAGIC     Activities2,      
# MAGIC     ActivitiesTimePercent3,
# MAGIC     Activities3,           
# MAGIC     ActivitiesTimePercent4,
# MAGIC     Activities4,           
# MAGIC     ActivitiesTimePercent5,
# MAGIC     Activities5,           
# MAGIC     ActivitiesTimePercent6,
# MAGIC     Activities6,           
# MAGIC     IngestionDate,         
# MAGIC     IsActive,              
# MAGIC     StartDate,             
# MAGIC     EndDate
# MAGIC )
# MAGIC SELECT * FROM Temp_DimActivities

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 Logic

# COMMAND ----------

# DBTITLE 1,Create Temp Staging Table
# Create the Source Data Frame
sourceDF = df_activities_noblehire_enriched
sourceDF.display()
print("Count: {}".format(sourceDF.count()))

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
deltaJobActivities = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimActivities")

targetDF = deltaJobActivities.toDF()
targetDF.display()

# COMMAND ----------

# DBTITLE 1,Join source and target
targetDF = targetDF.filter(col("IsActive") == True).select(*[c for c in targetDF.columns if c not in ["IsActive", "StartDate", "EndDate"]])


joinDF = (
    sourceDF
    .join(
        targetDF, 
        (sourceDF.ActivitiesId == targetDF.ActivitiesId),
#         & (targetDF.IsActive == "true"),
        "outer"
    )
    .select(
        sourceDF["*"],
        targetDF.ActivitiesId.alias("target_ActivitiesId"),
        targetDF.SourceSystem.alias("target_SourceSystem"),
        targetDF.ActivitiesTimePercent0.alias("target_ActivitiesTimePercent0"),
        targetDF.Activities0.alias("target_Activities0"),
        targetDF.ActivitiesTimePercent1.alias("target_ActivitiesTimePercent1"),
        targetDF.Activities1.alias("target_Activities1"),
        targetDF.ActivitiesTimePercent2.alias("target_ActivitiesTimePercent2"),
        targetDF.Activities2.alias("target_Activities2"),
        targetDF.ActivitiesTimePercent3.alias("target_ActivitiesTimePercent3"),
        targetDF.Activities3.alias("target_Activities3"),
        targetDF.ActivitiesTimePercent4.alias("target_ActivitiesTimePercent4"),
        targetDF.Activities4.alias("target_Activities4"),
        targetDF.ActivitiesTimePercent5.alias("target_ActivitiesTimePercent5"),
        targetDF.Activities5.alias("target_Activities5"),
        targetDF.ActivitiesTimePercent6.alias("target_ActivitiesTimePercent6"),
        targetDF.Activities6.alias("target_Activities6"),
        targetDF.IngestionDate.alias("target_IngestionDate")
    )
)

joinDF.display()

# COMMAND ----------

# DBTITLE 1,Hash source and target columns and compare them
filterDF = joinDF.filter(xxhash64(*[c for c in joinDF.columns if c.startswith("target") == False and "IngestionDate" not in c]) != xxhash64(*[c for c in joinDF.columns if c.startswith("target") == True and "IngestionDate" not in c])).withColumn("MergeKey", col("target_ActivitiesId"))

filterDF.display()

# COMMAND ----------

# DBTITLE 1,Add MergeKey and set it to null where Id is not null
dummyDF = filterDF.filter(col("target_ActivitiesId").isNotNull()).withColumn("MergeKey", lit(None))

dummyDF.display()

# COMMAND ----------

# DBTITLE 1,Union DFs
scdDF = filterDF.union(dummyDF)

scdDF.display()

# COMMAND ----------

# DBTITLE 1,Merge
(deltaJobActivities.alias("target")
 .merge(
     scdDF.alias("source"),
     "target.ActivitiesId = source.MergeKey"
 )
 .whenMatchedUpdate(set = 
    {
        "SourceSystem": "'Noblehire.io'",
        "IsActive": "'False'", 
        "EndDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
    }
 )
 .whenNotMatchedInsert(
     condition = "source.ActivitiesId IS NOT NULL",
     values =
     {
        "ActivitiesId": "source.ActivitiesId",
        "SourceSystem": "source.SourceSystem",
        "ActivitiesTimePercent0": "source.ActivitiesTimePercent0",
        "Activities0": "source.Activities0",
        "ActivitiesTimePercent1": "source.ActivitiesTimePercent1",
        "Activities1": "source.Activities1",
        "ActivitiesTimePercent2": "source.ActivitiesTimePercent2",
        "Activities2": "source.Activities2",
        "ActivitiesTimePercent3": "source.ActivitiesTimePercent3",
        "Activities3": "source.Activities3",
        "ActivitiesTimePercent4": "source.ActivitiesTimePercent4",
        "Activities4": "source.Activities4",
        "ActivitiesTimePercent5": "source.ActivitiesTimePercent5",
        "Activities5": "source.Activities5",
        "ActivitiesTimePercent6": "source.ActivitiesTimePercent6",
        "Activities6": "source.Activities6",
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
deltaJobActivities.history().display()

# COMMAND ----------

# DBTITLE 1,Compare Delta Table records with records in the Source DataFrame
# Raise error if there are records in the delta table (when filtered to show only active records), which do not exists in the source DataFrame
targetExceptSourceCount = df_activities_noblehire_enriched.where(col("IsActive") == True).select("ActivitiesId").exceptAll(sourceDF.select("ActivitiesId")).count()
targetEqualsSourceCount = df_activities_noblehire_enriched.where(col("IsActive") == True).count() == sourceDF.count()

if targetExceptSourceCount > 0 or targetEqualsSourceCount == False:
    raise Exception("There are records in source, which do not exist in target.")
