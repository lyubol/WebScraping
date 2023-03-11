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
# MAGIC ## Create DimTools

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data

# COMMAND ----------

# Read base
df_tools_noblehire_base = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Noblehire.io/base/jobTools/{current_year}/{current_month}/{current_day}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data

# COMMAND ----------

# Select and rename columns
df_tools_noblehire_enriched = (
    df_tools_noblehire_base
    .select(
        col("id").alias("ToolsId"),
        col("Source").alias("SourceSystem"),
        col("tools_0").alias("Tools0"),
        col("tools_1").alias("Tools1"),
        col("tools_2").alias("Tools2"),
        col("tools_3").alias("Tools3"),
        col("tools_4").alias("Tools4"),
        col("tools_5").alias("Tools5"),
        col("tools_6").alias("Tools6"),
        col("tools_7").alias("Tools7"),
        col("tools_8").alias("Tools8"),
        col("tools_9").alias("Tools9"),
        col("tools_10").alias("Tools10"),
        col("tools_11").alias("Tools11"),
        col("tools_12").alias("Tools12"),
        col("tools_13").alias("Tools13"),
        col("tools_14").alias("Tools14"), 
        col("tools_15").alias("Tools15"),
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
df_tools_noblehire_enriched = (
    df_tools_noblehire_enriched
    .withColumn("IsActive", lit(True))
    .withColumn("StartDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("EndDate", lit(None).cast(StringType()))
)

# COMMAND ----------

# DBTITLE 1,Populate Delta Table, if empty
df_tools_noblehire_enriched.createOrReplaceTempView("Temp_DimTools")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO WAREHOUSE.DimTools (  
# MAGIC   ToolsId,    
# MAGIC   SourceSystem,       
# MAGIC   Tools0,
# MAGIC   Tools1,
# MAGIC   Tools2,
# MAGIC   Tools3,
# MAGIC   Tools4,
# MAGIC   Tools5,
# MAGIC   Tools6,
# MAGIC   Tools7,
# MAGIC   Tools8,
# MAGIC   Tools9,
# MAGIC   Tools10,
# MAGIC   Tools11,
# MAGIC   Tools12,
# MAGIC   Tools13,
# MAGIC   Tools14,
# MAGIC   Tools15,
# MAGIC   IngestionDate,      
# MAGIC   IsActive,           
# MAGIC   StartDate,          
# MAGIC   EndDate            
# MAGIC )
# MAGIC SELECT * FROM Temp_DimTools

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 Logic

# COMMAND ----------

# DBTITLE 1,Create Temp Staging Table
# Create the Source Data Frame
sourceDF = df_tools_noblehire_enriched
sourceDF.display()
print("Count: {}".format(sourceDF.count()))

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
deltaJobTools = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimTools")

targetDF = deltaJobTools.toDF()
targetDF.display()

# COMMAND ----------

# DBTITLE 1,Join source and target
targetDF = targetDF.filter(col("IsActive") == True).select(*[c for c in targetDF.columns if c not in ["IsActive", "StartDate", "EndDate"]])


joinDF = (
    sourceDF
    .join(
        targetDF, 
        (sourceDF.ToolsId == targetDF.ToolsId),
#         & (targetDF.IsActive == "true"),
        "outer"
    )
    .select(
        sourceDF["*"],
        targetDF.ToolsId.alias("target_ToolsId"),
        targetDF.SourceSystem.alias("target_SourceSystem"),
        targetDF.Tools0.alias("target_Tools0"),
        targetDF.Tools1.alias("target_Tools1"),
        targetDF.Tools2.alias("target_Tools2"),
        targetDF.Tools3.alias("target_Tools3"),
        targetDF.Tools4.alias("target_Tools4"),
        targetDF.Tools5.alias("target_Tools5"),
        targetDF.Tools6.alias("target_Tools6"),
        targetDF.Tools7.alias("target_Tools7"),
        targetDF.Tools8.alias("target_Tools8"),
        targetDF.Tools9.alias("target_Tools9"),
        targetDF.Tools10.alias("target_Tools10"),
        targetDF.Tools11.alias("target_Tools11"),
        targetDF.Tools12.alias("target_Tools12"),
        targetDF.Tools13.alias("target_Tools13"),
        targetDF.Tools14.alias("target_Tools14"),
        targetDF.Tools15.alias("target_Tools15"),
        targetDF.IngestionDate.alias("target_IngestionDate")
    )
)

joinDF.display()

# COMMAND ----------

# DBTITLE 1,Hash source and target columns and compare them
filterDF = joinDF.filter(xxhash64(*[c for c in joinDF.columns if c.startswith("target") == False and "IngestionDate" not in c]) != xxhash64(*[c for c in joinDF.columns if c.startswith("target") == True and "IngestionDate" not in c])).withColumn("MergeKey", col("target_ToolsId"))

filterDF.display()

# COMMAND ----------

# DBTITLE 1,Add MergeKey and set it to null where Id is not null
dummyDF = filterDF.filter(col("target_ToolsId").isNotNull()).withColumn("MergeKey", lit(None))

dummyDF.display()

# COMMAND ----------

# DBTITLE 1,Union DFs
scdDF = filterDF.union(dummyDF)

scdDF.display()

# COMMAND ----------

# DBTITLE 1,Merge
(deltaJobTools.alias("target")
 .merge(
     scdDF.alias("source"),
     "target.ToolsId = source.MergeKey"
 )
 .whenMatchedUpdate(set = 
    {
        "SourceSystem": "'Noblehire.io'",
        "IsActive": "'False'", 
        "EndDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
    }
 )
 .whenNotMatchedInsert(
     condition = "source.ToolsId IS NOT NULL",
     values =
     {
        "ToolsId": "source.ToolsId",
        "SourceSystem": "source.SourceSystem",
        "Tools0": "source.Tools0",
        "Tools1": "source.Tools1",
        "Tools2": "source.Tools2",
        "Tools3": "source.Tools3",
        "Tools4": "source.Tools4",
        "Tools5": "source.Tools5",
        "Tools6": "source.Tools6",
        "Tools7": "source.Tools7",
        "Tools8": "source.Tools8",
        "Tools9": "source.Tools9",
        "Tools10": "source.Tools10",
        "Tools11": "source.Tools11",
        "Tools12": "source.Tools12",
        "Tools13": "source.Tools13",
        "Tools14": "source.Tools14",
        "Tools15": "source.Tools15",
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
deltaJobTools.history().display()

# COMMAND ----------

# DBTITLE 1,Compare Delta Table records with records in the Source DataFrame
# Read delta table into DataFrame
deltaFinal = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimTools")
finalTargetDF = deltaFinal.toDF()

# Raise error if there are records in the delta table (when filtered to show only active records), which do not exists in the source DataFrame
targetExceptSourceCount = finalTargetDF.where(col("IsActive") == True).select("ToolsId").exceptAll(sourceDF.select("ToolsId")).count()
targetEqualsSourceCount = finalTargetDF.where(col("IsActive") == True).count() == sourceDF.count()

if targetExceptSourceCount > 0 or targetEqualsSourceCount == False:
    raise Exception("There are records in source, which do not exist in target.")
