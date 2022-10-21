# Databricks notebook source
# MAGIC %md
# MAGIC ### This notebook is in progress and is currently used for testing the implementation of SCD Type 2

# COMMAND ----------

# DBTITLE 1,Imports
from datetime import date
import time
from pyspark.sql.types import *
from pyspark.sql.functions import *

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

# DBTITLE 1,Read Base
df_company_general = spark.read.format("parquet").load(main_path + company_general_path)
df_company_awards = spark.read.format("parquet").load(main_path + company_awards_path)
df_company_perks = spark.read.format("parquet").load(main_path + company_perks_path)
df_company_values = spark.read.format("parquet").load(main_path + company_values_path)
df_company_locations = spark.read.format("parquet").load(main_path + company_locations_path)
df_job_requirements = spark.read.format("parquet").load(main_path + job_requirements_path)
df_job_benefits = spark.read.format("parquet").load(main_path + job_benefits_path)
df_job_responsibilities = spark.read.format("parquet").load(main_path + job_responsibilities_path)
df_job_tools = spark.read.format("parquet").load(main_path + job_tools_path)
df_job_activities = spark.read.format("parquet").load(main_path + job_activities_path)
df_job_hiring_process = spark.read.format("parquet").load(main_path + job_hiring_process_path)
df_posts = spark.read.format("parquet").load(main_path + posts_path)

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

df_company_awards.createOrReplaceTempView("companyAwards")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM companyAwards

# COMMAND ----------

from pyspark.sql.functions import *

df_company_awards = (
    df_company_awards
    .withColumn("IsActive", lit(True))
    .withColumn("StartDate", current_timestamp())
    .withColumn("EndDate", lit("9999-12-31T00:00:00.000+0000"))
)

# COMMAND ----------

df_company_awards.display()

# COMMAND ----------

# DBTITLE 1,Create Delta Tables
# df_company_general.write.format("delta").saveAsTable("jobposts_noblehire.company_general")
df_company_awards.write.format("delta").saveAsTable("jobposts_noblehire.company_awards")
# df_company_perks.write.format("delta").saveAsTable("jobposts_noblehire.company_perks")
# df_company_values.write.format("delta").saveAsTable("jobposts_noblehire.company_values")
# df_company_locations.write.format("delta").saveAsTable("jobposts_noblehire.company_locations")
# df_job_requirements.write.format("delta").saveAsTable("jobposts_noblehire.job_requirements")
# df_job_benefits.write.format("delta").saveAsTable("jobposts_noblehire.job_benefits")
# df_job_responsibilities.write.format("delta").saveAsTable("jobposts_noblehire.job_responsibilities")
# df_job_tools.write.format("delta").saveAsTable("jobposts_noblehire.job_tools")
# df_job_activities.write.format("delta").saveAsTable("jobposts_noblehire.job_activities")
# df_job_hiring_process.write.format("delta").saveAsTable("jobposts_noblehire.job_hiring_process")
# df_posts.write.format("delta").saveAsTable("jobposts_noblehire.job_posts")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM jobposts_noblehire.company_awards
# MAGIC -- DROP TABLE jobposts_noblehire.company_awards

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DELETE FROM jobposts_noblehire.company_awards
# MAGIC WHERE companyId = 13
# MAGIC 
# MAGIC -- UPDATE jobposts_noblehire.company_awards
# MAGIC -- SET company_awards_title_0 = 'UPDATED total funding'
# MAGIC -- WHERE companyId = 10

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM companyAwards

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- SELECT
# MAGIC --   companyId AS MergeKey,
# MAGIC --   *
# MAGIC -- FROM
# MAGIC --   companyAwards
# MAGIC -- UNION ALL
# MAGIC SELECT
# MAGIC   NULL AS MergeKey,
# MAGIC   companyAwards.*
# MAGIC FROM
# MAGIC   companyAwards
# MAGIC   JOIN jobposts_noblehire.company_awards ON companyAwards.companyId = company_awards.companyId
# MAGIC WHERE
# MAGIC   company_awards.IsActive = true
# MAGIC   AND company_awards.company_awards_title_0 <> companyAwards.company_awards_title_0
# MAGIC   OR company_awards.company_awards_title_1 <> companyAwards.company_awards_title_1
# MAGIC   OR company_awards.company_awards_title_2 <> companyAwards.company_awards_title_2
# MAGIC   OR company_awards.company_awards_title_3 <> companyAwards.company_awards_title_3
# MAGIC   OR company_awards.company_awards_title_4 <> companyAwards.company_awards_title_4
# MAGIC   OR company_awards.company_awards_title_5 <> companyAwards.company_awards_title_5
# MAGIC   OR company_awards.company_awards_title_6 <> companyAwards.company_awards_title_6
# MAGIC   OR company_awards.company_awards_title_7 <> companyAwards.company_awards_title_7
# MAGIC   OR company_awards.company_awards_title_8 <> companyAwards.company_awards_title_8

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC MERGE INTO jobposts_noblehire.company_awards AS Target USING(
# MAGIC   SELECT
# MAGIC     companyId AS MergeKey,
# MAGIC     *
# MAGIC   FROM
# MAGIC     companyAwards
# MAGIC   UNION ALL
# MAGIC   SELECT
# MAGIC     NULL AS MergeKey,
# MAGIC     companyAwards.*
# MAGIC   FROM
# MAGIC     companyAwards
# MAGIC     JOIN jobposts_noblehire.company_awards ON companyAwards.companyId = company_awards.companyId
# MAGIC   WHERE
# MAGIC     company_awards.IsActive = true
# MAGIC     AND company_awards.company_awards_title_0 <> companyAwards.company_awards_title_0
# MAGIC     OR company_awards.company_awards_title_1 <> companyAwards.company_awards_title_1
# MAGIC     OR company_awards.company_awards_title_2 <> companyAwards.company_awards_title_2
# MAGIC     OR company_awards.company_awards_title_3 <> companyAwards.company_awards_title_3
# MAGIC     OR company_awards.company_awards_title_4 <> companyAwards.company_awards_title_4
# MAGIC     OR company_awards.company_awards_title_5 <> companyAwards.company_awards_title_5
# MAGIC     OR company_awards.company_awards_title_6 <> companyAwards.company_awards_title_6
# MAGIC     OR company_awards.company_awards_title_7 <> companyAwards.company_awards_title_7
# MAGIC     OR company_awards.company_awards_title_8 <> companyAwards.company_awards_title_8
# MAGIC ) AS Updates ON Target.companyId = Updates.MergeKey
# MAGIC WHEN MATCHED
# MAGIC AND Target.company_awards_title_0 <> Updates.company_awards_title_0
# MAGIC     OR Target.company_awards_title_1 <> Updates.company_awards_title_1
# MAGIC     OR Target.company_awards_title_2 <> Updates.company_awards_title_2
# MAGIC     OR Target.company_awards_title_3 <> Updates.company_awards_title_3
# MAGIC     OR Target.company_awards_title_4 <> Updates.company_awards_title_4
# MAGIC     OR Target.company_awards_title_5 <> Updates.company_awards_title_5
# MAGIC     OR Target.company_awards_title_6 <> Updates.company_awards_title_6
# MAGIC     OR Target.company_awards_title_7 <> Updates.company_awards_title_7
# MAGIC     OR Target.company_awards_title_8 <> Updates.company_awards_title_8
# MAGIC THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC   Target.Source = Updates.Source,
# MAGIC   Target.IngestionDate = Updates.IngestionDate,
# MAGIC   Target.IsActive = false,
# MAGIC   Target.EndDate = CURRENT_TIMESTAMP()
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   (
# MAGIC     companyId,
# MAGIC     company_awards_title_0,
# MAGIC     company_awards_title_1,
# MAGIC     company_awards_title_2,
# MAGIC     company_awards_title_3,
# MAGIC     company_awards_title_4,
# MAGIC     company_awards_title_5,
# MAGIC     company_awards_title_6,
# MAGIC     company_awards_title_7,
# MAGIC     company_awards_title_8,
# MAGIC     Source,
# MAGIC     IngestionDate,
# MAGIC     IsActive,
# MAGIC     StartDate,
# MAGIC     EndDate
# MAGIC   )
# MAGIC VALUES
# MAGIC   (
# MAGIC     Updates.companyId,
# MAGIC     Updates.company_awards_title_0,
# MAGIC     Updates.company_awards_title_1,
# MAGIC     Updates.company_awards_title_2,
# MAGIC     Updates.company_awards_title_3,
# MAGIC     Updates.company_awards_title_4,
# MAGIC     Updates.company_awards_title_5,
# MAGIC     Updates.company_awards_title_6,
# MAGIC     Updates.company_awards_title_7,
# MAGIC     Updates.company_awards_title_8,
# MAGIC     Updates.Source,
# MAGIC     Updates.IngestionDate,
# MAGIC     "true",
# MAGIC     CURRENT_TIMESTAMP(),
# MAGIC     "9999-12-31T00:00:00.000+0000"
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM jobposts_noblehire.company_awards

# COMMAND ----------

deltaCompanyAwards = spark.read.format("delta").load("/mnt/adlslirkov/it-job-boards/Noblehire.io/delta/company_awards")

deltaCompanyAwards.display()

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
from delta.tables import *

deltaCompanyAwards = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Noblehire.io/delta/company_awards")

targetDF = deltaCompanyAwards.toDF()
targetDF.display()

# COMMAND ----------

# DBTITLE 1,Read Update Data
sourceDF = df_company_awards.select(*[col for col in sourceDF.columns if col != "IngestionDate"])
sourceDF.display()

# COMMAND ----------

# DBTITLE 1,Join source and target
# Rename columns in targetDF, so that we don't need to manually alias them in the join below.
# Since this code will be used for DataFrames with different number of columns and column names, this is the approach that we need to take.
# targetDF = targetDF.toDF(*["target_" + column for column in targetDF.columns])

joinDF = (
    sourceDF
    .join(
        targetDF, 
        (sourceDF.companyId == targetDF.companyId) 
        & (targetDF.IsActive == "true"),
        "leftouter"
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
        targetDF.Source.alias("target_Source")
    )
)

joinDF.display()

# COMMAND ----------

# DBTITLE 1,Keep only records with changes
# df_company_awards_updates = (df_company_awards
#      .withColumn("MergeKey", lit(None))
#      .join(targetDF, "companyId", "fullouter")
#      .filter(
#         (targetDF.IsActive == "True") 
#         & (targetDF.company_awards_title_0 != df_company_awards.company_awards_title_0)
#         | (targetDF.company_awards_title_1 != df_company_awards.company_awards_title_1)
#         | (targetDF.company_awards_title_2 != df_company_awards.company_awards_title_2)
#         | (targetDF.company_awards_title_3 != df_company_awards.company_awards_title_3)
#         | (targetDF.company_awards_title_4 != df_company_awards.company_awards_title_4)
#         | (targetDF.company_awards_title_5 != df_company_awards.company_awards_title_5)
#         | (targetDF.company_awards_title_6 != df_company_awards.company_awards_title_6)
#         | (targetDF.company_awards_title_7 != df_company_awards.company_awards_title_7)
#         | (targetDF.company_awards_title_8 != df_company_awards.company_awards_title_8)
#      )
# )

# df_company_awards_updates = (
#     df_company_awards
#      .withColumn("MergeKey", lit(None))
#      .join(targetDF, "companyId", "fullouter")
# )

# COMMAND ----------

# DBTITLE 1,Hash source and target columns and compare them
filterDF = joinDF.filter(xxhash64(*[col for col in joinDF.columns if col.startswith("target") == False]) != xxhash64(*[col for col in joinDF.columns if col.startswith("target") == True]))

filterDF.display()

# COMMAND ----------

# dummyDF = filterDF.filter(col("target_companyId").isNotNull()).withColumn("MergeKey", lit(None))

# dummyDF.display()

# COMMAND ----------

(deltaCompanyAwards.alias("target")
 .merge(
     filterDF.alias("source"),
     "target.companyId = source.target_companyId"
 )
 .whenMatchedUpdate(set = 
    {
        "Source": "source.Source",
        "IngestionDate": "'None'",
        "IsActive": "'False'", 
        "EndDate": "current_date"
    }
 )
 .whenNotMatchedInsert(values =
     {
        "companyId": "source.companyId",
        "company_awards_title_0": "source.company_awards_title_0",
        "company_awards_title_1": "source.company_awards_title_1",
        "company_awards_title_2": "source.company_awards_title_2",
        "company_awards_title_3": "source.company_awards_title_3",
        "company_awards_title_4": "source.company_awards_title_4",
        "company_awards_title_5": "source.company_awards_title_5",
        "company_awards_title_6": "source.company_awards_title_6",
        "company_awards_title_7": "source.company_awards_title_7",
        "company_awards_title_8": "source.company_awards_title_8",
        "Source": "source.Source",
        "IngestionDate": "'None'",
        "IsActive": "'True'",
        "StartDate": "current_date",
        "EndDate": """to_date('9999-12-31', 'yyyy-MM-dd')"""
     }
 )
 .execute()
)

# COMMAND ----------

deltaCompanyAwards.history().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Temp Staging Tables

# COMMAND ----------

# DBTITLE 1,Create Temp Staging Tables
# df_company_general.createTempView("companyGeneral")
df_company_awards.createOrReplaceTempView("companyAwards")
df_company_perks.createOrReplaceTempView("companyPerks")
df_company_values.createOrReplaceTempView("companyValues")
df_company_locations.createOrReplaceTempView("companyLocations")
df_job_requirements.createOrReplaceTempView("jobRequirements")
df_job_benefits.createOrReplaceTempView("companyBenefits")
df_job_responsibilities.createOrReplaceTempView("jobResponsibilities")
df_job_tools.createOrReplaceTempView("jobTools")
df_job_activities.createOrReplaceTempView("jobActivities")
df_job_hiring_process.createOrReplaceTempView("jobHiringProcess")
df_posts.createOrReplaceTempView("Posts")

# COMMAND ----------

# DBTITLE 1,Company Activities
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM jobActivities

# COMMAND ----------

# DBTITLE 1,Company Benefits
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM companyBenefits

# COMMAND ----------

# DBTITLE 1,Company Awards
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM companyAwards

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC REFRESH TABLE companyAwards

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC MERGE INTO jobposts_noblehire.company_awards AS Target USING(
# MAGIC   SELECT
# MAGIC     companyId AS MergeKey,
# MAGIC     *
# MAGIC   FROM
# MAGIC     companyAwards
# MAGIC   UNION ALL
# MAGIC   SELECT
# MAGIC     NULL AS MergeKey,
# MAGIC     companyAwards.*
# MAGIC   FROM
# MAGIC     companyAwards
# MAGIC     JOIN jobposts_noblehire.company_awards ON companyAwards.companyId = company_awards.companyId
# MAGIC   WHERE
# MAGIC     company_awards.IsActive = true
# MAGIC     AND company_awards.company_awards_title_0 <> companyAwards.company_awards_title_0
# MAGIC     OR company_awards.company_awards_title_1 <> companyAwards.company_awards_title_1
# MAGIC     OR company_awards.company_awards_title_2 <> companyAwards.company_awards_title_2
# MAGIC     OR company_awards.company_awards_title_3 <> companyAwards.company_awards_title_3
# MAGIC     OR company_awards.company_awards_title_4 <> companyAwards.company_awards_title_4
# MAGIC     OR company_awards.company_awards_title_5 <> companyAwards.company_awards_title_5
# MAGIC     OR company_awards.company_awards_title_6 <> companyAwards.company_awards_title_6
# MAGIC     OR company_awards.company_awards_title_7 <> companyAwards.company_awards_title_7
# MAGIC     OR company_awards.company_awards_title_8 <> companyAwards.company_awards_title_8
# MAGIC ) AS Updates ON Target.companyId = Updates.MergeKey
# MAGIC WHEN MATCHED
# MAGIC AND Target.company_awards_title_0 <> Updates.company_awards_title_0
# MAGIC     OR Target.company_awards_title_1 <> Updates.company_awards_title_1
# MAGIC     OR Target.company_awards_title_2 <> Updates.company_awards_title_2
# MAGIC     OR Target.company_awards_title_3 <> Updates.company_awards_title_3
# MAGIC     OR Target.company_awards_title_4 <> Updates.company_awards_title_4
# MAGIC     OR Target.company_awards_title_5 <> Updates.company_awards_title_5
# MAGIC     OR Target.company_awards_title_6 <> Updates.company_awards_title_6
# MAGIC     OR Target.company_awards_title_7 <> Updates.company_awards_title_7
# MAGIC     OR Target.company_awards_title_8 <> Updates.company_awards_title_8
# MAGIC THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC   Target.Source = Updates.Source,
# MAGIC   Target.IngestionDate = Updates.IngestionDate,
# MAGIC   Target.IsActive = false,
# MAGIC   Target.EndDate = CURRENT_TIMESTAMP()
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   (
# MAGIC     companyId,
# MAGIC     company_awards_title_0,
# MAGIC     company_awards_title_1,
# MAGIC     company_awards_title_2,
# MAGIC     company_awards_title_3,
# MAGIC     company_awards_title_4,
# MAGIC     company_awards_title_5,
# MAGIC     company_awards_title_6,
# MAGIC     company_awards_title_7,
# MAGIC     company_awards_title_8,
# MAGIC     Source,
# MAGIC     IngestionDate,
# MAGIC     IsActive,
# MAGIC     StartDate,
# MAGIC     EndDate
# MAGIC   )
# MAGIC VALUES
# MAGIC   (
# MAGIC     Updates.companyId,
# MAGIC     Updates.company_awards_title_0,
# MAGIC     Updates.company_awards_title_1,
# MAGIC     Updates.company_awards_title_2,
# MAGIC     Updates.company_awards_title_3,
# MAGIC     Updates.company_awards_title_4,
# MAGIC     Updates.company_awards_title_5,
# MAGIC     Updates.company_awards_title_6,
# MAGIC     Updates.company_awards_title_7,
# MAGIC     Updates.company_awards_title_8,
# MAGIC     Updates.Source,
# MAGIC     Updates.IngestionDate,
# MAGIC     "true",
# MAGIC     CURRENT_TIMESTAMP(),
# MAGIC     "9999-12-31T00:00:00.000+0000"
# MAGIC   )

# COMMAND ----------

# DBTITLE 1,Company Perks
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM companyPerks

# COMMAND ----------

# DBTITLE 1,Company Values
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM companyValues

# COMMAND ----------

# DBTITLE 1,Company General
# MAGIC %sql
# MAGIC 
# MAGIC -- SELECT * FROM companyGeneral

# COMMAND ----------

# DBTITLE 1,Company Locations
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM companyLocations

# COMMAND ----------

# DBTITLE 1,Job Hiring Process
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM jobHiringProcess

# COMMAND ----------

# DBTITLE 1,Job Posts
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM Posts

# COMMAND ----------

# DBTITLE 1,Job Requirements
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM jobRequirements

# COMMAND ----------

# DBTITLE 1,Job Responsibilities
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM jobResponsibilities

# COMMAND ----------

# DBTITLE 1,Job Tools
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM jobTools

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Delta Tables

# COMMAND ----------


