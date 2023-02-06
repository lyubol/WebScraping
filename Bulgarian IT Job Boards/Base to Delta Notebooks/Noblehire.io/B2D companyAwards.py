# Databricks notebook source
# DBTITLE 1,Imports
from datetime import date
import time
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Define variables
# Date variables 
current_year = date.today().year
current_month = "0" + str(date.today().month) if len(str(date.today().month)) == 1 else date.today().month
current_day = "0" + str(date.today().day) if len(str(date.today().day)) == 1 else date.today().day

# Base location variables
location_prefix = "/dbfs"
main_path = "/mnt/adlslirkov/it-job-boards/Noblehire.io/base/"
temp_path = "/mnt/adlslirkov/it-job-boards/Noblehire.io/temp/"
company_general_path = f"companyGeneral/{current_year}/{current_month}/{current_day}/"
company_awards_path = f"companyAwards/{current_year}/{current_month}/{current_day}/"
company_perks_path = f"companyPerks/{current_year}/{current_month}/{current_day}/"
company_values_path = f"companyValues/{current_year}/{current_month}/{current_day}/"
company_locations_path = f"companyLocations/{current_year}/{current_month}/{current_day}/"
job_requirements_path = f"jobRequirements/{current_year}/{current_month}/{current_day}/"
job_benefits_path = f"jobBenefits/{current_year}/{current_month}/{current_day}/"
job_responsibilities_path = f"jobResponsibilities/{current_year}/{current_month}/{current_day}/"
job_tools_path = f"jobTools/{current_year}/{current_month}/{current_day}/"
job_activities_path = f"jobActivities/{current_year}/{current_month}/{current_day}/"
job_hiring_process_path = f"jobHiringProcess/{current_year}/{current_month}/{current_day}/"
posts_path = f"posts/{current_year}/{current_month}/{current_day}/"

# COMMAND ----------

# DBTITLE 1,Create Temp Staging Table
# Read Base
df_company_awards = spark.read.format("parquet").load(main_path + company_awards_path)

# Create the Source Data Frame
sourceDF = df_company_awards
sourceDF.display()
print("Count: {}".format(sourceDF.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Database and Tables

# COMMAND ----------

# DBTITLE 1,Create Database
# MAGIC %sql
# MAGIC 
# MAGIC -- CREATE DATABASE IF NOT EXISTS jobposts_noblehire 
# MAGIC -- COMMENT 'This database holds job posts data coming from Noblehire.io'
# MAGIC -- LOCATION '/mnt/adlslirkov/it-job-boards/Noblehire.io/delta/'

# COMMAND ----------

# DBTITLE 1,Add SCD Type 2 Columns to Delta Table
# # This command has been ran just once, when the delta table was first created.

# df_company_awards = (
#     df_company_awards
#     .withColumn("IsActive", lit(True))
#     .withColumn("StartDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
#     .withColumn("EndDate", lit(None).cast(StringType()))
# )

# COMMAND ----------

# DBTITLE 1,Create Delta Table
# # This command has been ran just once, when the delta table was first created.

# df_company_awards.write.format("delta").saveAsTable("jobposts_noblehire.company_awards")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM jobposts_noblehire.company_awards WHERE IsActive = True
# MAGIC -- DROP TABLE jobposts_noblehire.company_awards

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
deltaCompanyAwards = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Noblehire.io/delta/company_awards")

targetDF = deltaCompanyAwards.toDF()
targetDF.display()

# COMMAND ----------

# DBTITLE 1,Check for new columns in source
newColumns = [col for col in sourceDF.dtypes if col not in targetDF.dtypes]

print(newColumns)

# COMMAND ----------

# DBTITLE 1,Create new columns in target if any in source
if len(newColumns) > 0:
    for columnObject in newColumns:
        spark.sql("ALTER TABLE jobposts_noblehire.company_awards ADD COLUMN ({} {})".format(columnObject[0], columnObject[1]))
        print("Column {} of type {} have been added.".format(columnObject[0], columnObject[1]))
    else:
        deltaCompanyAwards = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Noblehire.io/delta/company_awards")
        targetDF = deltaCompanyAwards.toDF()
else:
    print("No new columns.")

# COMMAND ----------

# DBTITLE 1,Join source and target
# Rename columns in targetDF, so that we don't need to manually alias them in the join below.
# Since this code will be used for DataFrames with different number of columns and column names, this is the approach that we need to take.
# targetDF = targetDF.toDF(*["target_" + column for column in targetDF.columns])

targetDF = targetDF.filter(col("IsActive") == True).select(*[col for col in targetDF.columns if col not in ["IsActive", "StartDate", "EndDate"]])


joinDF = (
    sourceDF
    .join(
        targetDF, 
        (sourceDF.companyId == targetDF.companyId),
#         & (targetDF.IsActive == "true"),
        "outer"
    )
    .select(
        sourceDF["*"],
        targetDF.companyId.alias("target_companyId"),
        targetDF.company_awards_title_0.alias("target_company_awards_title_0"),
        targetDF.company_awards_title_1.alias("target_company_awards_title_1"),
        targetDF.company_awards_title_2.alias("target_company_awards_title_2"),
        targetDF.company_awards_title_3.alias("target_company_awards_title_3"),
        targetDF.company_awards_title_4.alias("target_company_awards_title_4"),
        targetDF.company_awards_title_5.alias("target_company_awards_title_5"),
        targetDF.company_awards_title_6.alias("target_company_awards_title_6"),
        targetDF.company_awards_title_7.alias("target_company_awards_title_7"),
        targetDF.company_awards_title_8.alias("target_company_awards_title_8"),
        targetDF.Source.alias("target_Source"),
        targetDF.IngestionDate.alias("target_IngestionDate")
    )
)

joinDF.display()

# COMMAND ----------

# DBTITLE 1,Hash source and target columns and compare them
filterDF = joinDF.filter(xxhash64(*[col for col in joinDF.columns if col.startswith("target") == False and "IngestionDate" not in col]) != xxhash64(*[col for col in joinDF.columns if col.startswith("target") == True and "IngestionDate" not in col])).withColumn("MergeKey", col("target_companyId"))

filterDF.display()

# COMMAND ----------

# DBTITLE 1,Add MergeKey and set it to null where Id is not null
dummyDF = filterDF.filter(col("target_companyId").isNotNull()).withColumn("MergeKey", lit(None))

dummyDF.display()

# COMMAND ----------

# DBTITLE 1,Union DFs
scdDF = filterDF.union(dummyDF)

scdDF.display()

# COMMAND ----------

# DBTITLE 1,Create Dictionary which will be used in the Merge Command
columns_dict = {col: "source." + col for col in df_company_awards.columns }
columns_dict["IsActive"] = "'True'"
columns_dict["StartDate"] = "current_timestamp"
# columns_dict["EndDate"] = """to_date('9999-12-31 00:00:00.0000', 'MM-dd-yyyy HH:mm:ss')"""

columns_dict

# COMMAND ----------

# DBTITLE 1,Merge
(deltaCompanyAwards.alias("target")
 .merge(
     scdDF.alias("source"),
     "target.companyId = source.MergeKey"
 )
 .whenMatchedUpdate(set = 
    {
        "Source": "'Noblehire.io'",
        "IsActive": "'False'", 
        "EndDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
    }
 )
 .whenNotMatchedInsert(
     condition = "source.companyId IS NOT NULL",
     values =
        columns_dict
#      {
#         "companyId": "source.companyId",
#         "company_awards_title_0": "source.company_awards_title_0",
#         "company_awards_title_1": "source.company_awards_title_1",
#         "company_awards_title_2": "source.company_awards_title_2",
#         "company_awards_title_3": "source.company_awards_title_3",
#         "company_awards_title_4": "source.company_awards_title_4",
#         "company_awards_title_5": "source.company_awards_title_5",
#         "company_awards_title_6": "source.company_awards_title_6",
#         "company_awards_title_7": "source.company_awards_title_7",
#         "company_awards_title_8": "source.company_awards_title_8",
#         "Source": "source.Source",
#         "IngestionDate": "source.IngestionDate",
#         "IsActive": "'True'",
#         "StartDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')",
#         "EndDate": """to_date('9999-12-31 00:00:00.0000', 'MM-dd-yyyy HH:mm:ss')"""
#      }
 )
 .execute()
)

# COMMAND ----------

# DBTITLE 1,Check Delta Table History
deltaCompanyAwards.history().display()

# COMMAND ----------

# DBTITLE 1,Compare Delta Table records with records in the Source DataFrame
# Read delta table into DataFrame
deltaFinalPosts = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Noblehire.io/delta/company_awards")
finalTargetDF = deltaFinalPosts.toDF()

# Raise error if there are records in the delta table (when filtered to show only active records), which do not exists in the source DataFrame
targetExceptSourceCount = finalTargetDF.where(col("IsActive") == True).select("companyId").exceptAll(sourceDF.select("companyId")).count()
targetEqualsSourceCount = finalTargetDF.where(col("IsActive") == True).count() == sourceDF.count()

if targetExceptSourceCount > 0 or targetEqualsSourceCount == False:
    raise Exception("There are records in source, which do not exist in target.")
