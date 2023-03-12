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
# MAGIC ## Create DimCompany

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data

# COMMAND ----------

# Read base
df_company_noblehire_base = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Noblehire.io/base/companyGeneral/{current_year}/{current_month}/{current_day}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data

# COMMAND ----------

# Select and rename columns
df_company_noblehire_enriched = (
    df_company_noblehire_base
    .select(
        col("companyId").alias("CompanyId"),
        col("Source").alias("SourceSystem"),
        col("brand").alias("CompanyName"),
        col("overview").alias("Overview"),
        col("product").alias("Product"),
        col("public").alias("IsPublic"),
        col("slug").alias("CompanySlug"),
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
df_company_noblehire_enriched = (
    df_company_noblehire_enriched
    .withColumn("IsActive", lit(True))
    .withColumn("StartDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("EndDate", lit(None).cast(StringType()))
)

# COMMAND ----------

# DBTITLE 1,Populate Delta Table, if empty
df_company_noblehire_enriched.createOrReplaceTempView("Temp_DimCompany")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO WAREHOUSE.DimCompany (  
# MAGIC   CompanyId,    
# MAGIC   SourceSystem,       
# MAGIC   CompanyName,
# MAGIC   Overview, 
# MAGIC   Product,
# MAGIC   IsPublic, 
# MAGIC   CompanySlug,
# MAGIC   IngestionDate,
# MAGIC   IsActive,           
# MAGIC   StartDate,          
# MAGIC   EndDate            
# MAGIC )
# MAGIC SELECT * FROM Temp_DimCompany

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 Logic

# COMMAND ----------

# DBTITLE 1,Create Temp Staging Table
# Create the Source Data Frame
sourceDF = df_company_noblehire_enriched
sourceDF.display()
print("Count: {}".format(sourceDF.count()))

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
deltaCompany = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimCompany")

targetDF = deltaCompany.toDF()
targetDF.display()

# COMMAND ----------

# DBTITLE 1,Join source and target
targetDF = targetDF.filter(col("IsActive") == True).select(*[c for c in targetDF.columns if c not in ["IsActive", "StartDate", "EndDate"]])


joinDF = (
    sourceDF
    .join(
        targetDF, 
        (sourceDF.CompanyId == targetDF.CompanyId),
#         & (targetDF.IsActive == "true"),
        "outer"
    )
    .select(
        sourceDF["*"],
        targetDF.CompanyId.alias("target_CompanyId"),
        targetDF.SourceSystem.alias("target_SourceSystem"),
        targetDF.CompanyName.alias("target_CompanyName"),
        targetDF.Overview.alias("target_Overview"),
        targetDF.Product.alias("target_Product"),
        targetDF.IsPublic.alias("target_IsPublic"),
        targetDF.CompanySlug.alias("target_CompanySlug"),
        targetDF.IngestionDate.alias("target_IngestionDate")
    )
)

joinDF.display()

# COMMAND ----------

# DBTITLE 1,Hash source and target columns and compare them
filterDF = joinDF.filter(xxhash64(*[c for c in joinDF.columns if c.startswith("target") == False and "IngestionDate" not in c]) != xxhash64(*[c for c in joinDF.columns if c.startswith("target") == True and "IngestionDate" not in c])).withColumn("MergeKey", col("target_CompanyId"))

filterDF.display()

# COMMAND ----------

# DBTITLE 1,Add MergeKey and set it to null where Id is not null
dummyDF = filterDF.filter(col("target_CompanyId").isNotNull()).withColumn("MergeKey", lit(None))

dummyDF.display()

# COMMAND ----------

# DBTITLE 1,Union DFs
scdDF = filterDF.union(dummyDF)

scdDF.display()

# COMMAND ----------

# DBTITLE 1,Merge
(deltaCompany.alias("target")
 .merge(
     scdDF.alias("source"),
     "target.CompanyId = source.MergeKey"
 )
 .whenMatchedUpdate(set = 
    {
        "SourceSystem": "'Noblehire.io'",
        "IsActive": "'False'", 
        "EndDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
    }
 )
 .whenNotMatchedInsert(
     condition = "source.CompanyId IS NOT NULL",
     values =
     {
        "CompanyId": "source.CompanyId",
        "SourceSystem": "source.SourceSystem",
        "CompanyName": "source.CompanyName",
        "Overview": "source.Overview",
        "Product": "source.Product",
        "IsPublic": "source.IsPublic",
        "CompanySlug": "source.CompanySlug",
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
deltaCompany.history().display()

# COMMAND ----------

# DBTITLE 1,Compare Delta Table records with records in the Source DataFrame
# Read delta table into DataFrame
deltaFinal = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimCompany")
finalTargetDF = deltaFinal.toDF()

# Raise error if there are records in the delta table (when filtered to show only active records), which do not exists in the source DataFrame
targetExceptSourceCount = finalTargetDF.where(col("IsActive") == True).select("CompanyId").exceptAll(sourceDF.select("CompanyId")).count()
targetEqualsSourceCount = finalTargetDF.where(col("IsActive") == True).count() == sourceDF.count()

if targetExceptSourceCount > 0 or targetEqualsSourceCount == False:
    raise Exception("There are records in source, which do not exist in target.")
