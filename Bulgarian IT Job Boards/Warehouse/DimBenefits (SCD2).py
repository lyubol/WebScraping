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
# MAGIC ## Create DimBenefits

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data

# COMMAND ----------

# Read base
df_benefits_noblehire_base = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Noblehire.io/base/jobBenefits/{current_year}/{current_month}/{current_day}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data

# COMMAND ----------

# Select and rename columns
df_benefits_noblehire_enriched = (
    df_benefits_noblehire_base
    .select(
        col("id").alias("BenefitsId"),
        col("Source").alias("SourceSystem"),
        col("benefits_0_title").alias("Benefits0"),
        col("benefits_1_title").alias("Benefits1"),
        col("benefits_2_title").alias("Benefits2"),
        col("benefits_3_title").alias("Benefits3"),
        col("benefits_4_title").alias("Benefits4"),
        col("benefits_5_title").alias("Benefits5"),
        col("benefits_6_title").alias("Benefits6"),
        col("benefits_7_title").alias("Benefits7"),
        col("benefits_8_title").alias("Benefits8"),
        col("benefits_9_title").alias("Benefits9"),
        col("benefits_10_title").alias("Benefits10"),
        col("benefits_11_title").alias("Benefits11"),
        col("benefits_12_title").alias("Benefits12"),
        col("benefits_13_title").alias("Benefits13"),
        col("benefits_14_title").alias("Benefits14"),
        col("benefits_15_title").alias("Benefits15"),
        col("benefits_16_title").alias("Benefits16"),
        col("benefits_17_title").alias("Benefits17"),
        col("benefits_18_title").alias("Benefits18"),
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
df_benefits_noblehire_enriched = (
    df_benefits_noblehire_enriched
    .withColumn("IsActive", lit(True))
    .withColumn("StartDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("EndDate", lit(None).cast(StringType()))
)

# COMMAND ----------

# DBTITLE 1,Populate Delta Table, if empty
df_benefits_noblehire_enriched.createOrReplaceTempView("Temp_DimBenefits")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO WAREHOUSE.DimBenefits (
# MAGIC     BenefitsId,   
# MAGIC     SourceSystem, 
# MAGIC     Benefits0,    
# MAGIC     Benefits1,    
# MAGIC     Benefits2,    
# MAGIC     Benefits3,    
# MAGIC     Benefits4,    
# MAGIC     Benefits5,    
# MAGIC     Benefits6,    
# MAGIC     Benefits7,    
# MAGIC     Benefits8,    
# MAGIC     Benefits9,    
# MAGIC     Benefits10,   
# MAGIC     Benefits11,   
# MAGIC     Benefits12,   
# MAGIC     Benefits13,   
# MAGIC     Benefits14,   
# MAGIC     Benefits15,   
# MAGIC     Benefits16,   
# MAGIC     Benefits17,   
# MAGIC     Benefits18,   
# MAGIC     IngestionDate,
# MAGIC     IsActive,     
# MAGIC     StartDate,    
# MAGIC     EndDate      
# MAGIC )
# MAGIC SELECT * FROM Temp_DimBenefits

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 Logic

# COMMAND ----------

# DBTITLE 1,Create Temp Staging Table
# Create the Source Data Frame
sourceDF = df_benefits_noblehire_enriched
sourceDF.display()
print("Count: {}".format(sourceDF.count()))

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
deltaJobBenefits = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimBenefits")

targetDF = deltaJobBenefits.toDF()
targetDF.display()

# COMMAND ----------

# DBTITLE 1,Join source and target
targetDF = targetDF.filter(col("IsActive") == True).select(*[c for c in targetDF.columns if c not in ["IsActive", "StartDate", "EndDate"]])


joinDF = (
    sourceDF
    .join(
        targetDF, 
        (sourceDF.BenefitsId == targetDF.BenefitsId),
#         & (targetDF.IsActive == "true"),
        "outer"
    )
    .select(
        sourceDF["*"],
        targetDF.BenefitsId.alias("target_BenefitsId"),
        targetDF.SourceSystem.alias("target_SourceSystem"),
        targetDF.Benefits0.alias("target_Benefits0"),
        targetDF.Benefits1.alias("target_Benefits1"),
        targetDF.Benefits2.alias("target_Benefits2"),
        targetDF.Benefits3.alias("target_Benefits3"),
        targetDF.Benefits4.alias("target_Benefits4"),
        targetDF.Benefits5.alias("target_Benefits5"),
        targetDF.Benefits6.alias("target_Benefits6"),
        targetDF.Benefits7.alias("target_Benefits7"),
        targetDF.Benefits8.alias("target_Benefits8"),
        targetDF.Benefits9.alias("target_Benefits9"),
        targetDF.Benefits10.alias("target_Benefits10"),
        targetDF.Benefits11.alias("target_Benefits11"),
        targetDF.Benefits12.alias("target_Benefits12"),
        targetDF.Benefits13.alias("target_Benefits13"),
        targetDF.Benefits14.alias("target_Benefits14"),
        targetDF.Benefits15.alias("target_Benefits15"),
        targetDF.Benefits16.alias("target_Benefits16"),
        targetDF.Benefits17.alias("target_Benefits17"),
        targetDF.Benefits18.alias("target_Benefits18"),
        targetDF.IngestionDate.alias("target_IngestionDate")
    )
)

joinDF.display()

# COMMAND ----------

# DBTITLE 1,Hash source and target columns and compare them
filterDF = joinDF.filter(xxhash64(*[c for c in joinDF.columns if c.startswith("target") == False and "IngestionDate" not in c]) != xxhash64(*[c for c in joinDF.columns if c.startswith("target") == True and "IngestionDate" not in c])).withColumn("MergeKey", col("target_BenefitsId"))

filterDF.display()

# COMMAND ----------

# DBTITLE 1,Add MergeKey and set it to null where Id is not null
dummyDF = filterDF.filter(col("target_BenefitsId").isNotNull()).withColumn("MergeKey", lit(None))

dummyDF.display()

# COMMAND ----------

# DBTITLE 1,Union DFs
scdDF = filterDF.union(dummyDF)

scdDF.display()

# COMMAND ----------

# DBTITLE 1,Merge
(deltaJobBenefits.alias("target")
 .merge(
     scdDF.alias("source"),
     "target.BenefitsId = source.MergeKey"
 )
 .whenMatchedUpdate(set = 
    {
        "SourceSystem": "'Noblehire.io'",
        "IsActive": "'False'", 
        "EndDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
    }
 )
 .whenNotMatchedInsert(
     condition = "source.BenefitsId IS NOT NULL",
     values =
     {
        "BenefitsId": "source.BenefitsId",
        "SourceSystem": "source.SourceSystem",
        "Benefits0": "source.Benefits0",
        "Benefits1": "source.Benefits1",
        "Benefits2": "source.Benefits2",
        "Benefits3": "source.Benefits3",
        "Benefits4": "source.Benefits4",
        "Benefits5": "source.Benefits5",
        "Benefits6": "source.Benefits6",
        "Benefits7": "source.Benefits7",
        "Benefits8": "source.Benefits8",
        "Benefits9": "source.Benefits9",
        "Benefits10": "source.Benefits10",
        "Benefits11": "source.Benefits11",
        "Benefits12": "source.Benefits12",
        "Benefits13": "source.Benefits13",
        "Benefits14": "source.Benefits14",
        "Benefits15": "source.Benefits15",
        "Benefits16": "source.Benefits16",
        "Benefits17": "source.Benefits17",
        "Benefits18": "source.Benefits18",
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
deltaJobBenefits.history().display()

# COMMAND ----------

# DBTITLE 1,Compare Delta Table records with records in the Source DataFrame
# Read delta table into DataFrame
deltaFinal = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimBenefits")
finalTargetDF = deltaFinal.toDF()

# Raise error if there are records in the delta table (when filtered to show only active records), which do not exists in the source DataFrame
targetExceptSourceCount = finalTargetDF.where(col("IsActive") == True).select("BenefitsId").exceptAll(sourceDF.select("BenefitsId")).count()
targetEqualsSourceCount = finalTargetDF.where(col("IsActive") == True).count() == sourceDF.count()

if targetExceptSourceCount > 0 or targetEqualsSourceCount == False:
    raise Exception("There are records in source, which do not exist in target.")
