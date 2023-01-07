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
df_job_benefits = spark.read.format("parquet").load(main_path + job_benefits_path)

# Create the Source Data Frame
sourceDF = df_job_benefits
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

# df_job_benefits = (
#     df_job_benefits
#     .withColumn("IsActive", lit(True))
#     .withColumn("StartDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
#     .withColumn("EndDate", lit(None).cast(StringType()))
# )

# COMMAND ----------

# DBTITLE 1,Create Delta Table
# # This command has been ran just once, when the delta table was first created.

# df_job_benefits.write.format("delta").saveAsTable("jobposts_noblehire.job_benefits")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM jobposts_noblehire.job_benefits WHERE IsActive = True
# MAGIC -- DROP TABLE jobposts_noblehire.job_benefits

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
deltaJobBenefits = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Noblehire.io/delta/job_benefits")

targetDF = deltaJobBenefits.toDF()
targetDF.display()

# COMMAND ----------

# DBTITLE 1,Check for new columns in source
newColumns = [col for col in sourceDF.dtypes if col not in targetDF.dtypes]

print(newColumns)

# COMMAND ----------

# DBTITLE 1,Create new columns in target if any in source
if len(newColumns) > 0:
    for columnObject in newColumns:
        spark.sql("ALTER TABLE jobposts_noblehire.posts ADD COLUMN ({} {})".format(columnObject[0], columnObject[1]))
        print("Column {} of type {} have been added.".format(columnObject[0], columnObject[1]))
    else:
        deltaPosts = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Noblehire.io/delta/job_benefits")
        targetDF = deltaPosts.toDF()
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
columns_dict = {col: "source." + col for col in df_job_benefits.columns}
columns_dict["IsActive"] = "'True'"
columns_dict["StartDate"] = "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
# columns_dict["EndDate"] = """to_date('9999-12-31 00:00:00.0000', 'MM-dd-yyyy HH:mm:ss')"""

columns_dict

# COMMAND ----------

# DBTITLE 1,Merge
(deltaJobBenefits.alias("target")
 .merge(
     scdDF.alias("source"),
     "target.id = source.MergeKey"
 )
 .whenMatchedUpdate(set = 
    {
        "Source": "source.Source",
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
#         "benefits_0_icon": "source.benefits_0_icon",
#         "benefits_0_title": "source.benefits_0_title",
#         "benefits_1_icon": "source.benefits_1_icon",
#         "benefits_1_title": "source.benefits_1_title",
#         "benefits_2_icon": "source.benefits_2_icon",
#         "benefits_2_title": "source.benefits_2_title",
#         "benefits_3_icon": "source.benefits_3_icon",
#         "benefits_3_title": "source.benefits_3_title",
#         "benefits_4_icon": "source.benefits_4_icon",
#         "benefits_4_title": "source.benefits_4_title",
#         "benefits_5_icon": "source.benefits_5_icon",
#         "benefits_5_title": "source.benefits_5_title",
#         "benefits_6_icon": "source.benefits_6_icon",
#         "benefits_6_title": "source.benefits_6_title",
#         "benefits_7_icon": "source.benefits_7_icon",
#         "benefits_7_title": "source.benefits_7_title",
#         "benefits_8_icon": "source.benefits_8_icon",
#         "benefits_8_title": "source.benefits_8_title",
#         "benefits_9_icon": "source.benefits_9_icon",
#         "benefits_9_title": "source.benefits_9_title",
#         "benefits_10_icon": "source.benefits_10_icon",
#         "benefits_10_title": "source.benefits_10_title",
#         "benefits_11_icon": "source.benefits_11_icon",
#         "benefits_11_title": "source.benefits_11_title",
#         "benefits_12_icon": "source.benefits_12_icon",
#         "benefits_12_title": "source.benefits_12_title",
#         "benefits_13_icon": "source.benefits_13_icon",
#         "benefits_13_title": "source.benefits_13_title",
#         "benefits_14_icon": "source.benefits_14_icon",
#         "benefits_14_title": "source.benefits_14_title",
#         "benefits_15_icon": "source.benefits_15_icon",
#         "benefits_15_title": "source.benefits_15_title",
#         "benefits_16_icon": "source.benefits_16_icon",
#         "benefits_16_title": "source.benefits_16_title",
#         "benefits_17_icon": "source.benefits_17_icon",
#         "benefits_17_title": "source.benefits_17_title",
#         "benefits_18_icon": "source.benefits_18_icon",
#         "benefits_18_title": "source.benefits_18_title",
#         "benefits_19_icon": "source.benefits_19_icon",
#         "benefits_19_title": "source.benefits_19_title",
#         "benefits_20_icon": "source.benefits_20_icon",
#         "benefits_20_title": "source.benefits_20_title",
#         "benefits_21_icon": "source.benefits_21_icon",
#         "benefits_21_title": "source.benefits_21_title",
#         "benefits_22_icon": "source.benefits_22_icon",
#         "benefits_22_title": "source.benefits_22_title",
#         "benefits_23_icon": "source.benefits_23_icon",
#         "benefits_23_title": "source.benefits_23_title",
#         "benefits_24_icon": "source.benefits_24_icon",
#         "benefits_24_title": "source.benefits_24_title",
#         "benefits_25_icon": "source.benefits_25_icon",
#         "benefits_25_title": "source.benefits_25_title",
#         "benefits_26_icon": "source.benefits_26_icon",
#         "benefits_26_title": "source.benefits_26_title",
#         "benefits_27_icon": "source.benefits_27_icon",
#         "benefits_27_title": "source.benefits_27_title",
#         "benefits_28_icon": "source.benefits_28_icon",
#         "benefits_28_title": "source.benefits_28_title",
#         "benefits_29_icon": "source.benefits_29_icon",
#         "benefits_29_title": "source.benefits_29_title",
#         "benefits_30_icon": "source.benefits_30_icon",
#         "benefits_30_title": "source.benefits_30_title",
#         "IsActive": "'True'",
#         "StartDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
#         "EndDate": """to_date('9999-12-31 00:00:00.0000', 'MM-dd-yyyy HH:mm:ss')"""
#      }
 )
 .execute()
)

# COMMAND ----------

# DBTITLE 1,Check Delta Table History
deltaJobBenefits.history().display()

# COMMAND ----------

# DBTITLE 1,Compare Delta Table records with records in the Source DataFrame
# Read delta table into DataFrame
deltaFinalPosts = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Noblehire.io/delta/job_benefits")
finalTargetDF = deltaFinalPosts.toDF()

# Raise error if there are records in the delta table (when filtered to show only active records), which do not exists in the source DataFrame
targetExceptSourceCount = finalTargetDF.where(col("IsActive") == True).select("id").exceptAll(sourceDF.select("id")).count()
targetEqualsSourceCount = finalTargetDF.where(col("IsActive") == True).count() == sourceDF.count()

if targetExceptSourceCount > 0 or targetEqualsSourceCount == False:
    raise Exception("There are records in source, which do not exist in target.")
