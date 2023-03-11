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
# MAGIC ## Create DimValues

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data

# COMMAND ----------

# Read base
df_values_noblehire_base = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Noblehire.io/base/companyValues/{current_year}/{current_month}/{current_day}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data

# COMMAND ----------

# Select and rename columns
df_values_noblehire_enriched = (
    df_values_noblehire_base
    .select(
        col("companyId").alias("ValuesId"),
        col("Source").alias("SourceSystem"),
        col("company_values_title_0").alias("ValuesTitle0"),
        col("company_values_text_0").alias("ValuesText0"),
        col("company_values_title_1").alias("ValuesTitle1"),
        col("company_values_text_1").alias("ValuesText1"),
        col("company_values_title_2").alias("ValuesTitle2"),
        col("company_values_text_2").alias("ValuesText2"),
        col("company_values_title_3").alias("ValuesTitle3"),
        col("company_values_text_3").alias("ValuesText3"),
        col("company_values_title_4").alias("ValuesTitle4"),
        col("company_values_text_4").alias("ValuesText4"),
        col("company_values_title_5").alias("ValuesTitle5"),
        col("company_values_text_5").alias("ValuesText5"), 
        col("company_values_title_6").alias("ValuesTitle6"),
        col("company_values_text_6").alias("ValuesText6"),
        col("company_values_title_7").alias("ValuesTitle7"),
        col("company_values_text_7").alias("ValuesText7"),
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
df_values_noblehire_enriched = (
    df_values_noblehire_enriched
    .withColumn("IsActive", lit(True))
    .withColumn("StartDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("EndDate", lit(None).cast(StringType()))
)

# COMMAND ----------

# DBTITLE 1,Populate Delta Table, if empty
df_values_noblehire_enriched.createOrReplaceTempView("Temp_DimValues")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO WAREHOUSE.DimValues (  
# MAGIC   ValuesId,    
# MAGIC   SourceSystem,       
# MAGIC   ValuesTitle0,
# MAGIC   ValuesText0, 
# MAGIC   ValuesTitle1,
# MAGIC   ValuesText1, 
# MAGIC   ValuesTitle2,
# MAGIC   ValuesText2, 
# MAGIC   ValuesTitle3,
# MAGIC   ValuesText3, 
# MAGIC   ValuesTitle4,
# MAGIC   ValuesText4, 
# MAGIC   ValuesTitle5,
# MAGIC   ValuesText5, 
# MAGIC   ValuesTitle6,
# MAGIC   ValuesText6, 
# MAGIC   ValuesTitle7,
# MAGIC   ValuesText7, 
# MAGIC   IngestionDate,      
# MAGIC   IsActive,           
# MAGIC   StartDate,          
# MAGIC   EndDate            
# MAGIC )
# MAGIC SELECT * FROM Temp_DimValues

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 Logic

# COMMAND ----------

# DBTITLE 1,Create Temp Staging Table
# Create the Source Data Frame
sourceDF = df_values_noblehire_enriched
sourceDF.display()
print("Count: {}".format(sourceDF.count()))

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
deltaCompanyValues = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimValues")

targetDF = deltaCompanyValues.toDF()
targetDF.display()

# COMMAND ----------

# DBTITLE 1,Join source and target
targetDF = targetDF.filter(col("IsActive") == True).select(*[c for c in targetDF.columns if c not in ["IsActive", "StartDate", "EndDate"]])


joinDF = (
    sourceDF
    .join(
        targetDF, 
        (sourceDF.ValuesId == targetDF.ValuesId),
#         & (targetDF.IsActive == "true"),
        "outer"
    )
    .select(
        sourceDF["*"],
        targetDF.ValuesId.alias("target_ValuesId"),
        targetDF.SourceSystem.alias("target_SourceSystem"),
        targetDF.ValuesTitle0.alias("target_ValuesTitle0"),
        targetDF.ValuesText0.alias("target_ValuesText0"),
        targetDF.ValuesTitle1.alias("target_ValuesTitle1"),
        targetDF.ValuesText1.alias("target_ValuesText1"),
        targetDF.ValuesTitle2.alias("target_ValuesTitle2"),
        targetDF.ValuesText2.alias("target_ValuesText2"),
        targetDF.ValuesTitle3.alias("target_ValuesTitle3"),
        targetDF.ValuesText3.alias("target_ValuesText3"),
        targetDF.ValuesTitle4.alias("target_ValuesTitle4"),
        targetDF.ValuesText4.alias("target_ValuesText4"),
        targetDF.ValuesTitle5.alias("target_ValuesTitle5"),
        targetDF.ValuesText5.alias("target_ValuesText5"),
        targetDF.ValuesTitle6.alias("target_ValuesTitle6"),
        targetDF.ValuesText6.alias("target_ValuesText6"),
        targetDF.ValuesTitle7.alias("target_ValuesTitle7"),
        targetDF.ValuesText7.alias("target_ValuesText7"),
        targetDF.IngestionDate.alias("target_IngestionDate")
    )
)

joinDF.display()

# COMMAND ----------

# DBTITLE 1,Hash source and target columns and compare them
filterDF = joinDF.filter(xxhash64(*[c for c in joinDF.columns if c.startswith("target") == False and "IngestionDate" not in c]) != xxhash64(*[c for c in joinDF.columns if c.startswith("target") == True and "IngestionDate" not in c])).withColumn("MergeKey", col("target_ValuesId"))

filterDF.display()

# COMMAND ----------

# DBTITLE 1,Add MergeKey and set it to null where Id is not null
dummyDF = filterDF.filter(col("target_ValuesId").isNotNull()).withColumn("MergeKey", lit(None))

dummyDF.display()

# COMMAND ----------

# DBTITLE 1,Union DFs
scdDF = filterDF.union(dummyDF)

scdDF.display()

# COMMAND ----------

# DBTITLE 1,Merge
(deltaCompanyValues.alias("target")
 .merge(
     scdDF.alias("source"),
     "target.ValuesId = source.MergeKey"
 )
 .whenMatchedUpdate(set = 
    {
        "SourceSystem": "'Noblehire.io'",
        "IsActive": "'False'", 
        "EndDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
    }
 )
 .whenNotMatchedInsert(
     condition = "source.ValuesId IS NOT NULL",
     values =
     {
        "ValuesId": "source.ValuesId",
        "SourceSystem": "source.SourceSystem",
        "ValuesTitle0": "source.ValuesTitle0",
        "ValuesText0": "source.ValuesText0",
        "ValuesTitle1": "source.ValuesTitle1",
        "ValuesText1": "source.ValuesText1",
        "ValuesTitle2": "source.ValuesTitle2",
        "ValuesText2": "source.ValuesText2",
        "ValuesTitle3": "source.ValuesTitle3",
        "ValuesText3": "source.ValuesText3",
        "ValuesTitle4": "source.ValuesTitle4",
        "ValuesText4": "source.ValuesText4",
        "ValuesTitle5": "source.ValuesTitle5",
        "ValuesText5": "source.ValuesText5",
        "ValuesTitle6": "source.ValuesTitle6",
        "ValuesText6": "source.ValuesText6",
        "ValuesTitle7": "source.ValuesTitle7",
        "ValuesText7": "source.ValuesText7",
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
deltaCompanyValues.history().display()

# COMMAND ----------

# DBTITLE 1,Compare Delta Table records with records in the Source DataFrame
# Read delta table into DataFrame
deltaFinal = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimValues")
finalTargetDF = deltaFinal.toDF()

# Raise error if there are records in the delta table (when filtered to show only active records), which do not exists in the source DataFrame
targetExceptSourceCount = finalTargetDF.where(col("IsActive") == True).select("ValuesId").exceptAll(sourceDF.select("ValuesId")).count()
targetEqualsSourceCount = finalTargetDF.where(col("IsActive") == True).count() == sourceDF.count()

if targetExceptSourceCount > 0 or targetEqualsSourceCount == False:
    raise Exception("There are records in source, which do not exist in target.")
