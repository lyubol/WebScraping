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
df_company_locations = spark.read.format("parquet").load(main_path + company_locations_path)

# Create the Source Data Frame
sourceDF = df_company_locations
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

# df_company_locations = (
#     df_company_locations
#     .withColumn("IsActive", lit(True))
#     .withColumn("StartDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
#     .withColumn("EndDate", lit(None).cast(StringType()))
# )

# COMMAND ----------

# DBTITLE 1,Create Delta Table
# # This command has been ran just once, when the delta table was first created.

# df_company_locations.write.format("delta").saveAsTable("jobposts_noblehire.company_locations")

# COMMAND ----------

# Command used for testing purposes

# %sql

# SELECT * FROM jobposts_noblehire.company_locations

# COMMAND ----------

# Command used for testing purposes

# %sql

# DROP TABLE jobposts_noblehire.company_locations

# COMMAND ----------

# Command used for testing purposes

# %sql

# DELETE FROM jobposts_noblehire.company_locations
# WHERE companyId = 4

# COMMAND ----------

# Command used for testing purposes

# %sql

# UPDATE jobposts_noblehire.company_locations
# SET locations_0_founded = 2023
# WHERE companyId = 1

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
deltaCompanyLocations = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Noblehire.io/delta/company_locations")

targetDF = deltaCompanyLocations.toDF()
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
        deltaPosts = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Noblehire.io/delta/company_locations")
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
dummyDF = filterDF.filter(col("id").isNotNull()).withColumn("MergeKey", lit(None))

dummyDF.display()

# COMMAND ----------

# DBTITLE 1,Union DFs
scdDF = filterDF.union(dummyDF)

scdDF.display()

# COMMAND ----------

# DBTITLE 1,Create Dictionary which will be used in the Merge Command
columns_dict = {col: "source." + col for col in df_company_locations.columns}
columns_dict["IsActive"] = "'True'"
columns_dict["StartDate"] = "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
# columns_dict["EndDate"] = """to_date('9999-12-31 00:00:00.0000', 'MM-dd-yyyy HH:mm:ss')"""

columns_dict

# COMMAND ----------

# DBTITLE 1,Merge
(deltaCompanyLocations.alias("target")
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
#         "companyId": "source.companyId",
#         "locations_0_comment": "source.locations_0_comment",
#         "locations_0_founded": "source.locations_0_founded",
#         "locations_0_id": "source.locations_0_id",
#         "locations_0_teamSize": "source.locations_0_teamSize",
#         "locations_1_comment": "source.locations_1_comment",
#         "locations_1_founded": "source.locations_1_founded",
#         "locations_1_id": "source.locations_1_id",
#         "locations_1_teamSize": "source.locations_1_teamSize",
#         "locations_2_comment": "source.locations_2_comment",
#         "locations_2_founded": "source.locations_2_founded",
#         "locations_2_id": "source.locations_2_id",
#         "locations_2_teamSize": "source.locations_2_teamSize",
#         "locations_3_comment": "source.locations_3_comment",
#         "locations_3_founded": "source.locations_3_founded",
#         "locations_3_id": "source.locations_3_id",
#         "locations_3_teamSize": "source.locations_3_teamSize",
#         "locations_4_comment": "source.locations_4_comment",
#         "locations_4_founded": "source.locations_4_founded",
#         "locations_4_id": "source.locations_4_id",
#         "locations_4_teamSize": "source.locations_4_teamSize",
#         "locations_0_address_formatted_address": "source.locations_0_address_formatted_address",
#         "locations_0_address_place_id": "source.locations_0_address_place_id",
#         "locations_0_address_location_type": "source.locations_0_address_location_type",
#         "locations_0_address_latitude": "source.locations_0_address_latitude",
#         "locations_0_address_longitude": "source.locations_0_address_longitude",
#         "locations_0_address_viewport_northeast_latitude": "source.locations_0_address_viewport_northeast_latitude",
#         "locations_0_address_viewport_northeast_longitude": "source.locations_0_address_viewport_northeast_longitude",
#         "locations_0_address_viewport_southwest_latitude": "source.locations_0_address_viewport_southwest_latitude",
#         "locations_0_address_viewport_southwest_longitude": "source.locations_0_address_viewport_southwest_longitude",
#         "locations_0_address_bounds_northeast_latitude": "source.locations_0_address_bounds_northeast_latitude",
#         "locations_0_address_bounds_northeast_longitude": "source.locations_0_address_bounds_northeast_longitude",
#         "locations_0_address_bounds_southwest_latitude": "source.locations_0_address_bounds_southwest_latitude",
#         "locations_0_address_bounds_southwest_longitude": "source.locations_0_address_bounds_southwest_longitude",
#         "locations_0_address_types_array_0": "source.locations_0_address_types_array_0",
#         "locations_0_address_types_array_1": "source.locations_0_address_types_array_1",
#         "locations_0_address_types_array_2": "source.locations_0_address_types_array_2",
#         "locations_0_address_address_components_array_0_long_name": "source.locations_0_address_address_components_array_0_long_name",
#         "locations_0_address_address_components_array_1_long_name": "source.locations_0_address_address_components_array_1_long_name",
#         "locations_0_address_address_components_array_2_long_name": "source.locations_0_address_address_components_array_2_long_name",
#         "locations_0_address_address_components_array_3_long_name": "source.locations_0_address_address_components_array_3_long_name",
#         "locations_0_address_address_components_array_4_long_name": "source.locations_0_address_address_components_array_4_long_name",
#         "locations_0_address_address_components_array_5_long_name": "source.locations_0_address_address_components_array_5_long_name",
#         "locations_0_address_address_components_array_6_long_name": "source.locations_0_address_address_components_array_6_long_name",
#         "locations_0_address_address_components_array_7_long_name": "source.locations_0_address_address_components_array_7_long_name",
#         "locations_0_address_address_components_array_0_short_name": "source.locations_0_address_address_components_array_0_short_name",
#         "locations_0_address_address_components_array_1_short_name": "source.locations_0_address_address_components_array_1_short_name",
#         "locations_0_address_address_components_array_2_short_name": "source.locations_0_address_address_components_array_2_short_name",
#         "locations_0_address_address_components_array_3_short_name": "source.locations_0_address_address_components_array_3_short_name",
#         "locations_0_address_address_components_array_4_short_name": "source.locations_0_address_address_components_array_4_short_name",
#         "locations_0_address_address_components_array_5_short_name": "source.locations_0_address_address_components_array_5_short_name",
#         "locations_0_address_address_components_array_6_short_name": "source.locations_0_address_address_components_array_6_short_name",
#         "locations_0_address_address_components_array_7_short_name": "source.locations_0_address_address_components_array_7_short_name",
#         "locations_0_address_address_components_array_0_types_0": "source.locations_0_address_address_components_array_0_types_0",
#         "locations_0_address_address_components_array_0_types_1": "source.locations_0_address_address_components_array_0_types_1",
#         "locations_0_address_address_components_array_1_types_0": "source.locations_0_address_address_components_array_1_types_0",
#         "locations_0_address_address_components_array_1_types_1": "source.locations_0_address_address_components_array_1_types_1",
#         "locations_0_address_address_components_array_2_types_0": "source.locations_0_address_address_components_array_2_types_0",
#         "locations_0_address_address_components_array_2_types_1": "source.locations_0_address_address_components_array_2_types_1",
#         "locations_0_address_address_components_array_2_types_2": "source.locations_0_address_address_components_array_2_types_2",
#         "locations_0_address_address_components_array_3_types_0": "source.locations_0_address_address_components_array_3_types_0",
#         "locations_0_address_address_components_array_3_types_1": "source.locations_0_address_address_components_array_3_types_1",
#         "locations_0_address_address_components_array_4_types_0": "source.locations_0_address_address_components_array_4_types_0",
#         "locations_0_address_address_components_array_4_types_1": "source.locations_0_address_address_components_array_4_types_1",
#         "locations_0_address_address_components_array_5_types_0": "source.locations_0_address_address_components_array_5_types_0",
#         "locations_0_address_address_components_array_5_types_1": "source.locations_0_address_address_components_array_5_types_1",
#         "locations_0_address_address_components_array_6_types_0": "source.locations_0_address_address_components_array_6_types_0",
#         "locations_0_address_address_components_array_6_types_1": "source.locations_0_address_address_components_array_6_types_1",
#         "locations_0_address_address_components_array_7_types_0": "source.locations_0_address_address_components_array_7_types_0",
#         "locations_1_address_formatted_address": "source.locations_1_address_formatted_address",
#         "locations_1_address_place_id": "source.locations_1_address_place_id",
#         "locations_1_address_location_type": "source.locations_1_address_location_type",
#         "locations_1_address_latitude": "source.locations_1_address_latitude",
#         "locations_1_address_longitude": "source.locations_1_address_longitude",
#         "locations_1_address_viewport_northeast_latitude": "source.locations_1_address_viewport_northeast_latitude",
#         "locations_1_address_viewport_northeast_longitude": "source.locations_1_address_viewport_northeast_longitude",
#         "locations_1_address_viewport_southwest_latitude": "source.locations_1_address_viewport_southwest_latitude",
#         "locations_1_address_viewport_southwest_longitude": "source.locations_1_address_viewport_southwest_longitude",
#         "locations_1_address_bounds_northeast_latitude": "source.locations_1_address_bounds_northeast_latitude",
#         "locations_1_address_bounds_northeast_longitude": "source.locations_1_address_bounds_northeast_longitude",
#         "locations_1_address_bounds_southwest_latitude": "source.locations_1_address_bounds_southwest_latitude",
#         "locations_1_address_bounds_southwest_longitude": "source.locations_1_address_bounds_southwest_longitude",
#         "locations_1_address_types_array_0": "source.locations_1_address_types_array_0",
#         "locations_1_address_types_array_1": "source.locations_1_address_types_array_1",
#         "locations_1_address_address_components_array_0_long_name": "source.locations_1_address_address_components_array_0_long_name",
#         "locations_1_address_address_components_array_1_long_name": "source.locations_1_address_address_components_array_1_long_name",
#         "locations_1_address_address_components_array_2_long_name": "source.locations_1_address_address_components_array_2_long_name",
#         "locations_1_address_address_components_array_3_long_name": "source.locations_1_address_address_components_array_3_long_name",
#         "locations_1_address_address_components_array_4_long_name": "source.locations_1_address_address_components_array_4_long_name",
#         "locations_1_address_address_components_array_5_long_name": "source.locations_1_address_address_components_array_5_long_name",
#         "locations_1_address_address_components_array_6_long_name": "source.locations_1_address_address_components_array_6_long_name",
#         "locations_1_address_address_components_array_0_short_name": "source.locations_1_address_address_components_array_0_short_name",
#         "locations_1_address_address_components_array_1_short_name": "source.locations_1_address_address_components_array_1_short_name",
#         "locations_1_address_address_components_array_2_short_name": "source.locations_1_address_address_components_array_2_short_name",
#         "locations_1_address_address_components_array_3_short_name": "source.locations_1_address_address_components_array_3_short_name",
#         "locations_1_address_address_components_array_4_short_name": "source.locations_1_address_address_components_array_4_short_name",
#         "locations_1_address_address_components_array_5_short_name": "source.locations_1_address_address_components_array_5_short_name",
#         "locations_1_address_address_components_array_6_short_name": "source.locations_1_address_address_components_array_6_short_name",
#         "locations_1_address_address_components_array_0_types_0": "source.locations_1_address_address_components_array_0_types_0",
#         "locations_1_address_address_components_array_0_types_1": "source.locations_1_address_address_components_array_0_types_1",
#         "locations_1_address_address_components_array_1_types_0": "source.locations_1_address_address_components_array_1_types_0",
#         "locations_1_address_address_components_array_1_types_1": "source.locations_1_address_address_components_array_1_types_1",
#         "locations_1_address_address_components_array_2_types_0": "source.locations_1_address_address_components_array_2_types_0",
#         "locations_1_address_address_components_array_2_types_1": "source.locations_1_address_address_components_array_2_types_1",
#         "locations_1_address_address_components_array_3_types_0": "source.locations_1_address_address_components_array_3_types_0",
#         "locations_1_address_address_components_array_3_types_1": "source.locations_1_address_address_components_array_3_types_1",
#         "locations_1_address_address_components_array_4_types_0": "source.locations_1_address_address_components_array_4_types_0",
#         "locations_1_address_address_components_array_4_types_1": "source.locations_1_address_address_components_array_4_types_1",
#         "locations_1_address_address_components_array_5_types_0": "source.locations_1_address_address_components_array_5_types_0",
#         "locations_1_address_address_components_array_5_types_1": "source.locations_1_address_address_components_array_5_types_1",
#         "locations_1_address_address_components_array_6_types_0": "source.locations_1_address_address_components_array_6_types_0",
#         "locations_2_address_formatted_address": "source.locations_2_address_formatted_address",
#         "locations_2_address_place_id": "source.locations_2_address_place_id",
#         "locations_2_address_location_type": "source.locations_2_address_location_type",
#         "locations_2_address_latitude": "source.locations_2_address_latitude",
#         "locations_2_address_longitude": "source.locations_2_address_longitude",
#         "locations_2_address_viewport_northeast_latitude": "source.locations_2_address_viewport_northeast_latitude",
#         "locations_2_address_viewport_northeast_longitude": "source.locations_2_address_viewport_northeast_longitude",
#         "locations_2_address_viewport_southwest_latitude": "source.locations_2_address_viewport_southwest_latitude",
#         "locations_2_address_viewport_southwest_longitude": "source.locations_2_address_viewport_southwest_longitude",
#         "locations_2_address_bounds_northeast_latitude": "source.locations_2_address_bounds_northeast_latitude",
#         "locations_2_address_bounds_northeast_longitude": "source.locations_2_address_bounds_northeast_longitude",
#         "locations_2_address_bounds_southwest_latitude": "source.locations_2_address_bounds_southwest_latitude",
#         "locations_2_address_bounds_southwest_longitude": "source.locations_2_address_bounds_southwest_longitude",
#         "locations_2_address_types_array_0": "source.locations_2_address_types_array_0",
#         "locations_2_address_types_array_1": "source.locations_2_address_types_array_1",
#         "locations_2_address_address_components_array_0_long_name": "source.locations_2_address_address_components_array_0_long_name",
#         "locations_2_address_address_components_array_1_long_name": "source.locations_2_address_address_components_array_1_long_name",
#         "locations_2_address_address_components_array_2_long_name": "source.locations_2_address_address_components_array_2_long_name",
#         "locations_2_address_address_components_array_3_long_name": "source.locations_2_address_address_components_array_3_long_name",
#         "locations_2_address_address_components_array_4_long_name": "source.locations_2_address_address_components_array_4_long_name",
#         "locations_2_address_address_components_array_5_long_name": "source.locations_2_address_address_components_array_5_long_name",
#         "locations_2_address_address_components_array_6_long_name": "source.locations_2_address_address_components_array_6_long_name",
#         "locations_2_address_address_components_array_0_short_name": "source.locations_2_address_address_components_array_0_short_name",
#         "locations_2_address_address_components_array_1_short_name": "source.locations_2_address_address_components_array_1_short_name",
#         "locations_2_address_address_components_array_2_short_name": "source.locations_2_address_address_components_array_2_short_name",
#         "locations_2_address_address_components_array_3_short_name": "source.locations_2_address_address_components_array_3_short_name",
#         "locations_2_address_address_components_array_4_short_name": "source.locations_2_address_address_components_array_4_short_name",
#         "locations_2_address_address_components_array_5_short_name": "source.locations_2_address_address_components_array_5_short_name",
#         "locations_2_address_address_components_array_6_short_name": "source.locations_2_address_address_components_array_6_short_name",
#         "locations_2_address_address_components_array_0_types_0": "source.locations_2_address_address_components_array_0_types_0",
#         "locations_2_address_address_components_array_0_types_1": "source.locations_2_address_address_components_array_0_types_1",
#         "locations_2_address_address_components_array_1_types_0": "source.locations_2_address_address_components_array_1_types_0",
#         "locations_2_address_address_components_array_1_types_1": "source.locations_2_address_address_components_array_1_types_1",
#         "locations_2_address_address_components_array_2_types_0": "source.locations_2_address_address_components_array_2_types_0",
#         "locations_2_address_address_components_array_2_types_1": "source.locations_2_address_address_components_array_2_types_1",
#         "locations_2_address_address_components_array_3_types_0": "source.locations_2_address_address_components_array_3_types_0",
#         "locations_2_address_address_components_array_3_types_1": "source.locations_2_address_address_components_array_3_types_1",
#         "locations_2_address_address_components_array_4_types_0": "source.locations_2_address_address_components_array_4_types_0",
#         "locations_2_address_address_components_array_4_types_1": "source.locations_2_address_address_components_array_4_types_1",
#         "locations_2_address_address_components_array_5_types_0": "source.locations_2_address_address_components_array_5_types_0",
#         "locations_2_address_address_components_array_5_types_1": "source.locations_2_address_address_components_array_5_types_1",
#         "locations_2_address_address_components_array_6_types_0": "source.locations_2_address_address_components_array_6_types_0",
#         "locations_3_address_formatted_address": "source.locations_3_address_formatted_address",
#         "locations_3_address_place_id": "source.locations_3_address_place_id",
#         "locations_3_address_location_type": "source.locations_3_address_location_type",
#         "locations_3_address_latitude": "source.locations_3_address_latitude",
#         "locations_3_address_longitude": "source.locations_3_address_longitude",
#         "locations_3_address_viewport_northeast_latitude": "source.locations_3_address_viewport_northeast_latitude",
#         "locations_3_address_viewport_northeast_longitude": "source.locations_3_address_viewport_northeast_longitude",
#         "locations_3_address_viewport_southwest_latitude": "source.locations_3_address_viewport_southwest_latitude",
#         "locations_3_address_viewport_southwest_longitude": "source.locations_3_address_viewport_southwest_longitude",
#         "locations_3_address_bounds_northeast_latitude": "source.locations_3_address_bounds_northeast_latitude",
#         "locations_3_address_bounds_northeast_longitude": "source.locations_3_address_bounds_northeast_longitude",
#         "locations_3_address_bounds_southwest_latitude": "source.locations_3_address_bounds_southwest_latitude",
#         "locations_3_address_bounds_southwest_longitude": "source.locations_3_address_bounds_southwest_longitude",
#         "locations_3_address_types_array_0": "source.locations_3_address_types_array_0",
#         "locations_3_address_types_array_1": "source.locations_3_address_types_array_1",
#         "locations_3_address_address_components_array_0_long_name": "source.locations_3_address_address_components_array_0_long_name",
#         "locations_3_address_address_components_array_1_long_name": "source.locations_3_address_address_components_array_1_long_name",
#         "locations_3_address_address_components_array_2_long_name": "source.locations_3_address_address_components_array_2_long_name",
#         "locations_3_address_address_components_array_0_short_name": "source.locations_3_address_address_components_array_0_short_name",
#         "locations_3_address_address_components_array_1_short_name": "source.locations_3_address_address_components_array_1_short_name",
#         "locations_3_address_address_components_array_2_short_name": "source.locations_3_address_address_components_array_2_short_name",
#         "locations_3_address_address_components_array_0_types_0": "source.locations_3_address_address_components_array_0_types_0",
#         "locations_3_address_address_components_array_0_types_1": "source.locations_3_address_address_components_array_0_types_1",
#         "locations_3_address_address_components_array_1_types_0": "source.locations_3_address_address_components_array_1_types_0",
#         "locations_3_address_address_components_array_1_types_1": "source.locations_3_address_address_components_array_1_types_1",
#         "locations_3_address_address_components_array_2_types_0": "source.locations_3_address_address_components_array_2_types_0",
#         "locations_3_address_address_components_array_2_types_1": "source.locations_3_address_address_components_array_2_types_1",
#         "locations_4_address_formatted_address": "source.locations_4_address_formatted_address",
#         "locations_4_address_place_id": "source.locations_4_address_place_id",
#         "locations_4_address_location_type": "source.locations_4_address_location_type",
#         "locations_4_address_latitude": "source.locations_4_address_latitude",
#         "locations_4_address_longitude": "source.locations_4_address_longitude",
#         "locations_4_address_viewport_northeast_latitude": "source.locations_4_address_viewport_northeast_latitude",
#         "locations_4_address_viewport_northeast_longitude": "source.locations_4_address_viewport_northeast_longitude",
#         "locations_4_address_viewport_southwest_latitude": "source.locations_4_address_viewport_southwest_latitude",
#         "locations_4_address_viewport_southwest_longitude": "source.locations_4_address_viewport_southwest_longitude",
#         "locations_4_address_bounds_northeast_latitude": "source.locations_4_address_bounds_northeast_latitude",
#         "locations_4_address_bounds_northeast_longitude": "source.locations_4_address_bounds_northeast_longitude",
#         "locations_4_address_bounds_southwest_latitude": "source.locations_4_address_bounds_southwest_latitude",
#         "locations_4_address_bounds_southwest_longitude": "source.locations_4_address_bounds_southwest_longitude",
#         "locations_4_address_types_array_0": "source.locations_4_address_types_array_0",
#         "locations_4_address_types_array_1": "source.locations_4_address_types_array_1",
#         "locations_4_address_address_components_array_0_long_name": "source.locations_4_address_address_components_array_0_long_name",
#         "locations_4_address_address_components_array_1_long_name": "source.locations_4_address_address_components_array_1_long_name",
#         "locations_4_address_address_components_array_2_long_name": "source.locations_4_address_address_components_array_2_long_name",
#         "locations_4_address_address_components_array_0_short_name": "source.locations_4_address_address_components_array_0_short_name",
#         "locations_4_address_address_components_array_1_short_name": "source.locations_4_address_address_components_array_1_short_name",
#         "locations_4_address_address_components_array_2_short_name": "source.locations_4_address_address_components_array_2_short_name",
#         "locations_4_address_address_components_array_0_types_0": "source.locations_4_address_address_components_array_0_types_0",
#         "locations_4_address_address_components_array_0_types_1": "source.locations_4_address_address_components_array_0_types_1",
#         "locations_4_address_address_components_array_1_types_0": "source.locations_4_address_address_components_array_1_types_0",
#         "locations_4_address_address_components_array_1_types_1": "source.locations_4_address_address_components_array_1_types_1",
#         "locations_4_address_address_components_array_2_types_0": "source.locations_4_address_address_components_array_2_types_0",
#         "locations_4_address_address_components_array_2_types_1": "source.locations_4_address_address_components_array_2_types_1",
#         "postedAt_Timestamp": "source.postedAt_Timestamp",
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
deltaCompanyLocations.history().display()

# COMMAND ----------


