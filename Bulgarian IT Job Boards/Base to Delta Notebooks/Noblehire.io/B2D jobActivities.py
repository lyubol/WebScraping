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
df_job_activities = spark.read.format("parquet").load(main_path + job_activities_path)

# Create the Source Data Frame
sourceDF = df_job_activities
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

# df_job_activities = (
#     df_job_activities
#     .withColumn("IsActive", lit(True))
#     .withColumn("StartDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
#     .withColumn("EndDate", lit(None).cast(StringType()))
# )

# COMMAND ----------

# DBTITLE 1,Create Delta Table
# # This command has been ran just once, when the delta table was first created.

# df_job_activities.write.format("delta").saveAsTable("jobposts_noblehire.job_activities")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM jobposts_noblehire.job_activities WHERE IsActive = True
# MAGIC -- DROP TABLE jobposts_noblehire.job_activities

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
deltaJobActivities = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Noblehire.io/delta/job_activities")

targetDF = deltaJobActivities.toDF()
targetDF.display()

# COMMAND ----------

# DBTITLE 1,Check for new columns in source
newColumns = [col for col in sourceDF.dtypes if col not in targetDF.dtypes]

print(newColumns)

# COMMAND ----------

# DBTITLE 1,Create new columns in target if any in source
if len(newColumns) > 0:
    for columnObject in newColumns:
        spark.sql("ALTER TABLE jobposts_noblehire.job_activities ADD COLUMN ({} {})".format(columnObject[0], columnObject[1]))
        print("Column {} of type {} have been added.".format(columnObject[0], columnObject[1]))
    else:
        deltaJobActivities = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Noblehire.io/delta/job_activities")
        targetDF = deltaJobActivities.toDF()
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
        (sourceDF.id == targetDF.id),
#         & (targetDF.IsActive == "true"),
        "outer"
    )
    .select(
        sourceDF["*"],
        *[targetDF[f"{c}"].alias(f"target_{c}") for c in targetDF.columns]
#         targetDF.companyId.alias("target_companyId"),
#         targetDF.company_values_text_0.alias("target_company_values_text_0"),
#         targetDF.company_values_text_1.alias("target_company_values_text_1"),
#         targetDF.company_values_text_2.alias("target_company_values_text_2"),
#         targetDF.company_values_text_3.alias("target_company_values_text_3"),
#         targetDF.company_values_text_4.alias("target_company_values_text_4"),
#         targetDF.company_values_text_5.alias("target_company_values_text_5"),
#         targetDF.company_values_text_6.alias("target_company_values_text_6"),
#         targetDF.company_values_text_7.alias("target_company_values_text_7"),
#         targetDF.company_values_text_8.alias("target_company_values_text_8"),
#         targetDF.company_values_text_9.alias("target_company_values_text_9"),
#         targetDF.company_values_title_0.alias("target_company_values_title_0"),
#         targetDF.company_values_title_1.alias("target_company_values_title_1"),
#         targetDF.company_values_title_2.alias("target_company_values_title_2"),
#         targetDF.company_values_title_3.alias("target_company_values_title_3"),
#         targetDF.company_values_title_4.alias("target_company_values_title_4"),
#         targetDF.company_values_title_5.alias("target_company_values_title_5"),
#         targetDF.company_values_title_6.alias("target_company_values_title_6"),
#         targetDF.company_values_title_7.alias("target_company_values_title_7"),
#         targetDF.company_values_title_8.alias("target_company_values_title_8"),
#         targetDF.company_values_title_9.alias("target_company_values_title_9"),
#         targetDF.Source.alias("target_Source"),
#         targetDF.IngestionDate.alias("target_IngestionDate")
    )
)

joinDF.display()

# COMMAND ----------

# DBTITLE 1,Hash source and target columns and compare them
filterDF = joinDF.filter(xxhash64(*[col for col in joinDF.columns if col.startswith("target") == False and "IngestionDate" not in col]) != xxhash64(*[col for col in joinDF.columns if col.startswith("target") == True and "IngestionDate" not in col])).withColumn("MergeKey", col("target_id"))

filterDF.display()

# COMMAND ----------

# DBTITLE 1,Add MergeKey and set it to null where Id is not null
dummyDF = filterDF.filter(col("target_id").isNotNull()).withColumn("MergeKey", lit(None))

dummyDF.display()

# COMMAND ----------

# DBTITLE 1,Union DFs
scdDF = filterDF.union(dummyDF)

scdDF.display()

# COMMAND ----------

# DBTITLE 1,Create Dictionary which will be used in the Merge Command
columns_dict = {col: "source." + col for col in df_job_activities.columns}
columns_dict["IsActive"] = "'True'"
columns_dict["StartDate"] = "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
# columns_dict["EndDate"] = """to_date('9999-12-31 00:00:00.0000', 'MM-dd-yyyy HH:mm:ss')"""

columns_dict

# COMMAND ----------

# DBTITLE 1,Merge
(deltaJobActivities.alias("target")
 .merge(
     scdDF.alias("source"),
     "target.id = source.MergeKey"
 )
 .whenMatchedUpdate(set = 
    {
        "Source": "'Noblehire.io'",
        "IsActive": "'False'", 
        "EndDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
    }
 )
 .whenNotMatchedInsert(
     condition = "source.id IS NOT NULL",
     values =
        columns_dict
#      {
#         "id": "source.id",
#         "companyId": "source.companyId",
#         "Source": "source.Source",
#         "IngestionDate": "source.IngestionDate",
#         "postedAt_Timestamp": "source.postedAt_Timestamp",
#         "activities_0_timePercents": "source.activities_0_timePercents",
#         "activities_0_title": "source.activities_0_title",
#         "activities_1_timePercents": "source.activities_1_timePercents",
#         "activities_1_title": "source.activities_1_title",
#         "activities_2_timePercents": "source.activities_2_timePercents",
#         "activities_2_title": "source.activities_2_title",
#         "activities_3_timePercents": "source.activities_3_timePercents",
#         "activities_3_title": "source.activities_3_title",
#         "activities_4_timePercents": "source.activities_4_timePercents",
#         "activities_4_title": "source.activities_4_title",
#         "activities_5_timePercents": "source.activities_5_timePercents",
#         "activities_5_title": "source.activities_5_title",
#         "activities_6_timePercents": "source.activities_6_timePercents",
#         "activities_6_title": "source.activities_6_title",
#         "IsActive": "'True'",
#         "StartDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
# #         "EndDate": """to_date('9999-12-31 00:00:00.0000', 'MM-dd-yyyy HH:mm:ss')"""
#      }
 )
 .execute()
)

# COMMAND ----------

# DBTITLE 1,Check Delta Table History
deltaJobActivities.history().display()

# COMMAND ----------

# DBTITLE 1,Compare Delta Table records with records in the Source DataFrame
# Read delta table into DataFrame
deltaFinalPosts = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Noblehire.io/delta/job_activities")
finalTargetDF = deltaFinalPosts.toDF()

# Raise error if there are records in the delta table (when filtered to show only active records), which do not exists in the source DataFrame
targetExceptSourceCount = finalTargetDF.where(col("IsActive") == True).select("id").exceptAll(sourceDF.select("id")).count()
targetEqualsSourceCount = finalTargetDF.where(col("IsActive") == True).count() == sourceDF.count()

if targetExceptSourceCount > 0 or targetEqualsSourceCount == False:
    raise Exception("There are records in source, which do not exist in target.")
