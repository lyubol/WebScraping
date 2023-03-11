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
# MAGIC ## Create DimResponsibilities

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data

# COMMAND ----------

# Read base
df_responsibilities_noblehire_base = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Noblehire.io/base/jobResponsibilities/{current_year}/{current_month}/{current_day}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data

# COMMAND ----------

# Select and rename columns
df_responsibilities_noblehire_enriched = (
    df_responsibilities_noblehire_base
    .select(
        col("id").alias("ResponsibilitiesId"),
        col("Source").alias("SourceSystem"),
        col("responsibilities_0_title").alias("Responsibilities0"),
        col("responsibilities_1_title").alias("Responsibilities1"),
        col("responsibilities_2_title").alias("Responsibilities2"),
        col("responsibilities_3_title").alias("Responsibilities3"),
        col("responsibilities_4_title").alias("Responsibilities4"),
        col("responsibilities_5_title").alias("Responsibilities5"),
        col("responsibilities_6_title").alias("Responsibilities6"),
        col("responsibilities_7_title").alias("Responsibilities7"),
        col("responsibilities_8_title").alias("Responsibilities8"),
        col("responsibilities_9_title").alias("Responsibilities9"),
        col("responsibilities_10_title").alias("Responsibilities10"),
        col("responsibilities_11_title").alias("Responsibilities11"),
        col("responsibilities_12_title").alias("Responsibilities12"),
        col("responsibilities_13_title").alias("Responsibilities13"),
        col("responsibilities_14_title").alias("Responsibilities14"), 
        col("responsibilities_15_title").alias("Responsibilities15"),
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
df_responsibilities_noblehire_enriched = (
    df_responsibilities_noblehire_enriched
    .withColumn("IsActive", lit(True))
    .withColumn("StartDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("EndDate", lit(None).cast(StringType()))
)

# COMMAND ----------

# DBTITLE 1,Populate Delta Table, if empty
df_responsibilities_noblehire_enriched.createOrReplaceTempView("Temp_DimResponsibilities")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO WAREHOUSE.DimResponsibilities (  
# MAGIC   ResponsibilitiesId,    
# MAGIC   SourceSystem,       
# MAGIC   Responsibilities0,
# MAGIC   Responsibilities1,
# MAGIC   Responsibilities2,
# MAGIC   Responsibilities3,
# MAGIC   Responsibilities4,
# MAGIC   Responsibilities5,
# MAGIC   Responsibilities6,
# MAGIC   Responsibilities7,
# MAGIC   Responsibilities8,
# MAGIC   Responsibilities9,
# MAGIC   Responsibilities10,
# MAGIC   Responsibilities11,
# MAGIC   Responsibilities12,
# MAGIC   Responsibilities13,
# MAGIC   Responsibilities14,
# MAGIC   Responsibilities15,
# MAGIC   IngestionDate,      
# MAGIC   IsActive,           
# MAGIC   StartDate,          
# MAGIC   EndDate            
# MAGIC )
# MAGIC SELECT * FROM Temp_DimResponsibilities

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 Logic

# COMMAND ----------

# DBTITLE 1,Create Temp Staging Table
# Create the Source Data Frame
sourceDF = df_responsibilities_noblehire_enriched
sourceDF.display()
print("Count: {}".format(sourceDF.count()))

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
deltaJobResponsibilities = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimResponsibilities")

targetDF = deltaJobResponsibilities.toDF()
targetDF.display()

# COMMAND ----------

# DBTITLE 1,Join source and target
targetDF = targetDF.filter(col("IsActive") == True).select(*[c for c in targetDF.columns if c not in ["IsActive", "StartDate", "EndDate"]])


joinDF = (
    sourceDF
    .join(
        targetDF, 
        (sourceDF.ResponsibilitiesId == targetDF.ResponsibilitiesId),
#         & (targetDF.IsActive == "true"),
        "outer"
    )
    .select(
        sourceDF["*"],
        targetDF.ResponsibilitiesId.alias("target_ResponsibilitiesId"),
        targetDF.SourceSystem.alias("target_SourceSystem"),
        targetDF.Responsibilities0.alias("target_Responsibilities0"),
        targetDF.Responsibilities1.alias("target_Responsibilities1"),
        targetDF.Responsibilities2.alias("target_Responsibilities2"),
        targetDF.Responsibilities3.alias("target_Responsibilities3"),
        targetDF.Responsibilities4.alias("target_Responsibilities4"),
        targetDF.Responsibilities5.alias("target_Responsibilities5"),
        targetDF.Responsibilities6.alias("target_Responsibilities6"),
        targetDF.Responsibilities7.alias("target_Responsibilities7"),
        targetDF.Responsibilities8.alias("target_Responsibilities8"),
        targetDF.Responsibilities9.alias("target_Responsibilities9"),
        targetDF.Responsibilities10.alias("target_Responsibilities10"),
        targetDF.Responsibilities11.alias("target_Responsibilities11"),
        targetDF.Responsibilities12.alias("target_Responsibilities12"),
        targetDF.Responsibilities13.alias("target_Responsibilities13"),
        targetDF.Responsibilities14.alias("target_Responsibilities14"),
        targetDF.Responsibilities15.alias("target_Responsibilities15"),
        targetDF.IngestionDate.alias("target_IngestionDate")
    )
)

joinDF.display()

# COMMAND ----------

# DBTITLE 1,Hash source and target columns and compare them
filterDF = joinDF.filter(xxhash64(*[c for c in joinDF.columns if c.startswith("target") == False and "IngestionDate" not in c]) != xxhash64(*[c for c in joinDF.columns if c.startswith("target") == True and "IngestionDate" not in c])).withColumn("MergeKey", col("target_ResponsibilitiesId"))

filterDF.display()

# COMMAND ----------

# DBTITLE 1,Add MergeKey and set it to null where Id is not null
dummyDF = filterDF.filter(col("target_ResponsibilitiesId").isNotNull()).withColumn("MergeKey", lit(None))

dummyDF.display()

# COMMAND ----------

# DBTITLE 1,Union DFs
scdDF = filterDF.union(dummyDF)

scdDF.display()

# COMMAND ----------

# DBTITLE 1,Merge
(deltaJobResponsibilities.alias("target")
 .merge(
     scdDF.alias("source"),
     "target.ResponsibilitiesId = source.MergeKey"
 )
 .whenMatchedUpdate(set = 
    {
        "SourceSystem": "'Noblehire.io'",
        "IsActive": "'False'", 
        "EndDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
    }
 )
 .whenNotMatchedInsert(
     condition = "source.ResponsibilitiesId IS NOT NULL",
     values =
     {
        "ResponsibilitiesId": "source.ResponsibilitiesId",
        "SourceSystem": "source.SourceSystem",
        "Responsibilities0": "source.Responsibilities0",
        "Responsibilities1": "source.Responsibilities1",
        "Responsibilities2": "source.Responsibilities2",
        "Responsibilities3": "source.Responsibilities3",
        "Responsibilities4": "source.Responsibilities4",
        "Responsibilities5": "source.Responsibilities5",
        "Responsibilities6": "source.Responsibilities6",
        "Responsibilities7": "source.Responsibilities7",
        "Responsibilities8": "source.Responsibilities8",
        "Responsibilities9": "source.Responsibilities9",
        "Responsibilities10": "source.Responsibilities10",
        "Responsibilities11": "source.Responsibilities11",
        "Responsibilities12": "source.Responsibilities12",
        "Responsibilities13": "source.Responsibilities13",
        "Responsibilities14": "source.Responsibilities14",
        "Responsibilities15": "source.Responsibilities15",
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
deltaJobResponsibilities.history().display()

# COMMAND ----------

# DBTITLE 1,Compare Delta Table records with records in the Source DataFrame
# Read delta table into DataFrame
deltaFinal = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimResponsibilities")
finalTargetDF = deltaFinal.toDF()

# Raise error if there are records in the delta table (when filtered to show only active records), which do not exists in the source DataFrame
targetExceptSourceCount = finalTargetDF.where(col("IsActive") == True).select("ResponsibilitiesId").exceptAll(sourceDF.select("ResponsibilitiesId")).count()
targetEqualsSourceCount = finalTargetDF.where(col("IsActive") == True).count() == sourceDF.count()

if targetExceptSourceCount > 0 or targetEqualsSourceCount == False:
    raise Exception("There are records in source, which do not exist in target.")
