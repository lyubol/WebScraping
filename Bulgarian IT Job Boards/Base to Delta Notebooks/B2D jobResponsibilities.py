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
df_job_responsibilities = spark.read.format("parquet").load(main_path + job_responsibilities_path)

# Create the Source Data Frame
sourceDF = df_job_responsibilities
sourceDF.display()

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
# This command has been ran just once, when the delta table was first created.

# df_job_responsibilities = (
#     df_job_responsibilities
#     .withColumn("IsActive", lit(True))
#     .withColumn("StartDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
#     .withColumn("EndDate", lit(None).cast(StringType()))
# )

# COMMAND ----------

# DBTITLE 1,Create Delta Table
# # This command has been ran just once, when the delta table was first created.

# df_job_responsibilities.write.format("delta").saveAsTable("jobposts_noblehire.job_responsibilities")

# COMMAND ----------

# Command used for testing purposes

# %sql

# SELECT * FROM jobposts_noblehire.job_responsibilities

# COMMAND ----------

# Command used for testing purposes

# %sql

# DROP TABLE jobposts_noblehire.job_responsibilities

# COMMAND ----------

# Command used for testing purposes

# %sql

# DELETE FROM jobposts_noblehire.job_responsibilities
# WHERE companyId = 1

# COMMAND ----------

# Command used for testing purposes

# %sql

# UPDATE jobposts_noblehire.job_responsibilities
# SET responsibilities_0_title = 'Not a real responsibility.'
# WHERE companyId = 2

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
deltaJobResponsibilities = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Noblehire.io/delta/job_responsibilities")

targetDF = deltaJobResponsibilities.toDF()
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
        deltaPosts = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Noblehire.io/delta/job_responsibilities")
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
        "leftouter"
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
columns_dict = {col: "source." + col for col in df_job_responsibilities.columns}
columns_dict["IsActive"] = "'True'"
columns_dict["StartDate"] = "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
# columns_dict["EndDate"] = """to_date('9999-12-31 00:00:00.0000', 'MM-dd-yyyy HH:mm:ss')"""

columns_dict

# COMMAND ----------

# DBTITLE 1,Merge
(deltaJobResponsibilities.alias("target")
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
 .whenNotMatchedInsert(values =
        columns_dict
#      {
#         "id": "source.id",
#         "companyId": "source.companyId",
#         "Source": "source.Source",
#         "IngestionDate": "source.IngestionDate",
#         "postedAt_Timestamp": "source.postedAt_Timestamp",
#         "responsibilities_0_icon": "source.responsibilities_0_icon",
#         "responsibilities_0_title": "source.responsibilities_0_title",
#         "responsibilities_1_icon": "source.responsibilities_1_icon",
#         "responsibilities_1_title": "source.responsibilities_1_title",
#         "responsibilities_2_icon": "source.responsibilities_2_icon",
#         "responsibilities_2_title": "source.responsibilities_2_title",
#         "responsibilities_3_icon": "source.responsibilities_3_icon",
#         "responsibilities_3_title": "source.responsibilities_3_title",
#         "responsibilities_4_icon": "source.responsibilities_4_icon",
#         "responsibilities_4_title": "source.responsibilities_4_title",
#         "responsibilities_5_icon": "source.responsibilities_5_icon",
#         "responsibilities_5_title": "source.responsibilities_5_title",
#         "responsibilities_6_icon": "source.responsibilities_6_icon",
#         "responsibilities_6_title": "source.responsibilities_6_title",
#         "responsibilities_7_icon": "source.responsibilities_7_icon",
#         "responsibilities_7_title": "source.responsibilities_7_title",
#         "responsibilities_8_icon": "source.responsibilities_8_icon",
#         "responsibilities_8_title": "source.responsibilities_8_title",
#         "responsibilities_9_icon": "source.responsibilities_9_icon",
#         "responsibilities_9_title": "source.responsibilities_9_title",
#         "responsibilities_10_icon": "source.responsibilities_10_icon",
#         "responsibilities_10_title": "source.responsibilities_10_title",
#         "responsibilities_11_icon": "source.responsibilities_11_icon",
#         "responsibilities_11_title": "source.responsibilities_11_title",
#         "responsibilities_12_icon": "source.responsibilities_12_icon",
#         "responsibilities_12_title": "source.responsibilities_12_title",
#         "responsibilities_13_icon": "source.responsibilities_13_icon",
#         "responsibilities_13_title": "source.responsibilities_13_title",
#         "responsibilities_14_icon": "source.responsibilities_14_icon",
#         "responsibilities_14_title": "source.responsibilities_14_title",
#         "responsibilities_15_icon": "source.responsibilities_15_icon",
#         "responsibilities_15_title": "source.responsibilities_15_title",
#         "responsibilities_16_icon": "source.responsibilities_16_icon",
#         "responsibilities_16_title": "source.responsibilities_16_title",
#         "responsibilities_17_icon": "source.responsibilities_17_icon",
#         "responsibilities_17_title": "source.responsibilities_17_title",
#         "IsActive": "'True'",
#         "StartDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
#         "EndDate": """to_date('9999-12-31 00:00:00.0000', 'MM-dd-yyyy HH:mm:ss')"""
#      }
 )
 .execute()
)

# COMMAND ----------

# DBTITLE 1,Check Delta Table History
deltaJobResponsibilities.history().display()

# COMMAND ----------


