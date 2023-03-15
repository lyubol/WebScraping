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
# MAGIC ## Create DimAwards

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data

# COMMAND ----------

# Read base
df_awards_noblehire_base = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Noblehire.io/base/companyAwards/{current_year}/{current_month}/{current_day}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data

# COMMAND ----------

# Rename columns
df_awards_noblehire_enriched = (
    df_awards_noblehire_base
    .toDF("AwardsId", *[c.replace("company_", "").replace("_title", "").replace("_", "").title() for c in df_awards_noblehire_base.columns if c.startswith("company_")], "SourceSystem", "IngestionDate")
)

# Reorder columns
df_awards_noblehire_enriched = df_awards_noblehire_enriched.select("AwardsId", "SourceSystem", *[c for c in df_awards_noblehire_enriched.columns if c not in ["AwardsId", "AwardsKey", "SourceSystem", "IngestionDate"]], "IngestionDate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 Setup

# COMMAND ----------

# MAGIC %md
# MAGIC The following commands are used for the initial setup of the delta table

# COMMAND ----------

# DBTITLE 1,Add SCD Type 2 Columns to Delta Table
# df_awards_noblehire_enriched = (
#     df_awards_noblehire_enriched
#     .withColumn("IsActive", lit(True))
#     .withColumn("StartDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
#     .withColumn("EndDate", lit(None).cast(StringType()))
# )

# COMMAND ----------

# DBTITLE 1,Populate Delta Table, if empty
# df_awards_noblehire_enriched.createOrReplaceTempView("Temp_DimAwards")

# COMMAND ----------

# %sql

# INSERT INTO WAREHOUSE.DimAwards (AwardsId, SourceSystem, Awards0, Awards1, Awards2, Awards3, Awards4, Awards5, Awards6, Awards7, Awards8, IngestionDate, IsActive, StartDate, EndDate)
# SELECT * FROM Temp_DimAwards

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 Logic

# COMMAND ----------

# DBTITLE 1,Create Temp Staging Table
# Create the Source Data Frame
sourceDF = df_awards_noblehire_enriched
sourceDF.display()
print("Count: {}".format(sourceDF.count()))

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
deltaCompanyAwards = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimAwards")

targetDF = deltaCompanyAwards.toDF()
targetDF.display()

# COMMAND ----------

# DBTITLE 1,Join source and target
targetDF = targetDF.filter(col("IsActive") == True).select(*[c for c in targetDF.columns if c not in ["IsActive", "StartDate", "EndDate"]])


joinDF = (
    sourceDF
    .join(
        targetDF, 
        (sourceDF.AwardsId == targetDF.AwardsId),
#         & (targetDF.IsActive == "true"),
        "outer"
    )
    .select(
        sourceDF["*"],
        targetDF.AwardsId.alias("target_AwardsId"),
        targetDF.SourceSystem.alias("target_SourceSystem"),
        targetDF.Awards0.alias("target_Awards0"),
        targetDF.Awards1.alias("target_Awards1"),
        targetDF.Awards2.alias("target_Awards2"),
        targetDF.Awards3.alias("target_Awards3"),
        targetDF.Awards4.alias("target_Awards4"),
        targetDF.Awards5.alias("target_Awards5"),
        targetDF.Awards6.alias("target_Awards6"),
        targetDF.Awards7.alias("target_Awards7"),
        targetDF.Awards8.alias("target_Awards8"),
        targetDF.IngestionDate.alias("target_IngestionDate")
    )
)

joinDF.display()

# COMMAND ----------

# DBTITLE 1,Hash source and target columns and compare them
filterDF = joinDF.filter(xxhash64(*[c for c in joinDF.columns if c.startswith("target") == False and "IngestionDate" not in c]) != xxhash64(*[c for c in joinDF.columns if c.startswith("target") == True and "IngestionDate" not in c])).withColumn("MergeKey", col("target_AwardsId"))

filterDF.display()

# COMMAND ----------

# DBTITLE 1,Add MergeKey and set it to null where Id is not null
dummyDF = filterDF.filter(col("target_AwardsId").isNotNull()).withColumn("MergeKey", lit(None))

dummyDF.display()

# COMMAND ----------

# DBTITLE 1,Union DFs
scdDF = filterDF.union(dummyDF)

scdDF.display()

# COMMAND ----------

# DBTITLE 1,Merge
(deltaCompanyAwards.alias("target")
 .merge(
     scdDF.alias("source"),
     "target.AwardsId = source.MergeKey"
 )
 .whenMatchedUpdate(set = 
    {
        "SourceSystem": "'Noblehire.io'",
        "IsActive": "'False'", 
        "EndDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
    }
 )
 .whenNotMatchedInsert(
     condition = "source.AwardsId IS NOT NULL",
     values =
     {
        "AwardsId": "source.AwardsId",
        "SourceSystem": "source.SourceSystem",
        "Awards0": "source.Awards0",
        "Awards1": "source.Awards1",
        "Awards2": "source.Awards2",
        "Awards3": "source.Awards3",
        "Awards4": "source.Awards4",
        "Awards5": "source.Awards5",
        "Awards6": "source.Awards6",
        "Awards7": "source.Awards7",
        "Awards8": "source.Awards8",
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
deltaCompanyAwards.history().display()

# COMMAND ----------

# DBTITLE 1,Compare Delta Table records with records in the Source DataFrame
# Read delta table into DataFrame
deltaFinal = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimAwards")
finalTargetDF = deltaFinal.toDF()

# Raise error if there are records in the delta table (when filtered to show only active records), which do not exists in the source DataFrame
targetExceptSourceCount = finalTargetDF.where(col("IsActive") == True).select("AwardsId").exceptAll(sourceDF.select("AwardsId")).count()
targetEqualsSourceCount = finalTargetDF.where(col("IsActive") == True).count() == sourceDF.count()

if targetExceptSourceCount > 0 or targetEqualsSourceCount == False:
    raise Exception("There are records in source, which do not exist in target.")
