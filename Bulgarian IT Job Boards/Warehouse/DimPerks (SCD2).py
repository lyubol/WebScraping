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
df_perks_noblehire_base = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Noblehire.io/base/companyPerks/{current_year}/{current_month}/{current_day}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data

# COMMAND ----------

# Select and rename columns
df_perks_noblehire_enriched = (
    df_perks_noblehire_base
    .select(
        col("companyId").alias("PerksId"),
        col("Source").alias("SourceSystem"),
        col("company_perks_text_0").alias("Perks0"),
        col("company_perks_text_1").alias("Perks1"),
        col("company_perks_text_2").alias("Perks2"),
        col("company_perks_text_3").alias("Perks3"),
        col("company_perks_text_4").alias("Perks4"),
        col("company_perks_text_5").alias("Perks5"),
        col("company_perks_text_6").alias("Perks6"),
        col("company_perks_text_7").alias("Perks7"),
        col("company_perks_text_8").alias("Perks8"),
        col("company_perks_text_9").alias("Perks9"),
        col("company_perks_text_10").alias("Perks10"),
        col("company_perks_text_11").alias("Perks11"),
        col("company_perks_text_12").alias("Perks12"),
        col("company_perks_text_13").alias("Perks13"),
        col("company_perks_text_14").alias("Perks14"),
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
df_perks_noblehire_enriched = (
    df_perks_noblehire_enriched
    .withColumn("IsActive", lit(True))
    .withColumn("StartDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("EndDate", lit(None).cast(StringType()))
)

# COMMAND ----------

# DBTITLE 1,Populate Delta Table, if empty
df_perks_noblehire_enriched.createOrReplaceTempView("Temp_DimPerks")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO WAREHOUSE.DimPerks (  
# MAGIC   PerksId,    
# MAGIC   SourceSystem,       
# MAGIC   Perks0,
# MAGIC   Perks1,
# MAGIC   Perks2,
# MAGIC   Perks3,
# MAGIC   Perks4,
# MAGIC   Perks5,
# MAGIC   Perks6,
# MAGIC   Perks7,
# MAGIC   Perks8,
# MAGIC   Perks9,
# MAGIC   Perks10,
# MAGIC   Perks11,
# MAGIC   Perks12,
# MAGIC   Perks13,
# MAGIC   Perks14,
# MAGIC   IngestionDate,      
# MAGIC   IsActive,           
# MAGIC   StartDate,          
# MAGIC   EndDate            
# MAGIC )
# MAGIC SELECT * FROM Temp_DimPerks

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 Logic

# COMMAND ----------

# DBTITLE 1,Create Temp Staging Table
# Create the Source Data Frame
sourceDF = df_perks_noblehire_enriched
sourceDF.display()
print("Count: {}".format(sourceDF.count()))

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
deltaCompanyPerks = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimPerks")

targetDF = deltaCompanyPerks.toDF()
targetDF.display()

# COMMAND ----------

# DBTITLE 1,Join source and target
targetDF = targetDF.filter(col("IsActive") == True).select(*[c for c in targetDF.columns if c not in ["IsActive", "StartDate", "EndDate"]])


joinDF = (
    sourceDF
    .join(
        targetDF, 
        (sourceDF.PerksId == targetDF.PerksId),
#         & (targetDF.IsActive == "true"),
        "outer"
    )
    .select(
        sourceDF["*"],
        targetDF.PerksId.alias("target_PerksId"),
        targetDF.SourceSystem.alias("target_SourceSystem"),
        targetDF.Perks0.alias("target_Perks0"),
        targetDF.Perks1.alias("target_Perks1"),
        targetDF.Perks2.alias("target_Perks2"),
        targetDF.Perks3.alias("target_Perks3"),
        targetDF.Perks4.alias("target_Perks4"),
        targetDF.Perks5.alias("target_Perks5"),
        targetDF.Perks6.alias("target_Perks6"),
        targetDF.Perks7.alias("target_Perks7"),
        targetDF.Perks8.alias("target_Perks8"),
        targetDF.Perks9.alias("target_Perks9"),
        targetDF.Perks10.alias("target_Perks10"),
        targetDF.Perks11.alias("target_Perks11"),
        targetDF.Perks12.alias("target_Perks12"),
        targetDF.Perks13.alias("target_Perks13"),
        targetDF.Perks14.alias("target_Perks14"),
        targetDF.IngestionDate.alias("target_IngestionDate")
    )
)

joinDF.display()

# COMMAND ----------

# DBTITLE 1,Hash source and target columns and compare them
filterDF = joinDF.filter(xxhash64(*[c for c in joinDF.columns if c.startswith("target") == False and "IngestionDate" not in c]) != xxhash64(*[c for c in joinDF.columns if c.startswith("target") == True and "IngestionDate" not in c])).withColumn("MergeKey", col("target_PerksId"))

filterDF.display()

# COMMAND ----------

# DBTITLE 1,Add MergeKey and set it to null where Id is not null
dummyDF = filterDF.filter(col("target_PerksId").isNotNull()).withColumn("MergeKey", lit(None))

dummyDF.display()

# COMMAND ----------

# DBTITLE 1,Union DFs
scdDF = filterDF.union(dummyDF)

scdDF.display()

# COMMAND ----------

# DBTITLE 1,Merge
(deltaCompanyPerks.alias("target")
 .merge(
     scdDF.alias("source"),
     "target.PerksId = source.MergeKey"
 )
 .whenMatchedUpdate(set = 
    {
        "SourceSystem": "'Noblehire.io'",
        "IsActive": "'False'", 
        "EndDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
    }
 )
 .whenNotMatchedInsert(
     condition = "source.PerksId IS NOT NULL",
     values =
     {
        "PerksId": "source.PerksId",
        "SourceSystem": "source.SourceSystem",
        "Perks0": "source.Perks0",
        "Perks1": "source.Perks1",
        "Perks2": "source.Perks2",
        "Perks3": "source.Perks3",
        "Perks4": "source.Perks4",
        "Perks5": "source.Perks5",
        "Perks6": "source.Perks6",
        "Perks7": "source.Perks7",
        "Perks8": "source.Perks8",
        "Perks9": "source.Perks9",
        "Perks10": "source.Perks10",
        "Perks11": "source.Perks11",
        "Perks12": "source.Perks12",
        "Perks13": "source.Perks13",
        "Perks14": "source.Perks14",
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
deltaCompanyPerks.history().display()

# COMMAND ----------

# DBTITLE 1,Compare Delta Table records with records in the Source DataFrame
# Read delta table into DataFrame
deltaFinal = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimPerks")
finalTargetDF = deltaFinal.toDF()

# Raise error if there are records in the delta table (when filtered to show only active records), which do not exists in the source DataFrame
targetExceptSourceCount = finalTargetDF.where(col("IsActive") == True).select("PerksId").exceptAll(sourceDF.select("PerksId")).count()
targetEqualsSourceCount = finalTargetDF.where(col("IsActive") == True).count() == sourceDF.count()

if targetExceptSourceCount > 0 or targetEqualsSourceCount == False:
    raise Exception("There are records in source, which do not exist in target.")
