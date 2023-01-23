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
main_path = "/mnt/adlslirkov/it-job-boards/Zaplata.bg/base/"
posts_path = f"posts/{current_year}/{current_month}/{current_day}/"

# COMMAND ----------

# DBTITLE 1,Create Temp Staging Table
# Read Base
df_posts = spark.read.format("parquet").load(main_path + posts_path)

# Create the Source Data Frame
sourceDF = df_posts
sourceDF.display()
print("Count: {}".format(sourceDF.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Database and Tables

# COMMAND ----------

# DBTITLE 1,Create Database
# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS jobposts_zaplatabg
# MAGIC COMMENT 'This database holds job posts data coming from Zaplata.bg'
# MAGIC LOCATION '/mnt/adlslirkov/it-job-boards/Zaplata.bg/delta/'

# COMMAND ----------

# DBTITLE 1,Add SCD Type 2 Columns to Delta Table
# # This command has been ran just once, when the delta table was first created.

# df_posts = (
#     df_posts
#     .withColumn("IsActive", lit(True))
#     .withColumn("StartDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
#     .withColumn("EndDate", lit(None).cast(StringType()))
# )

# COMMAND ----------

# DBTITLE 1,Create Delta Table
# # This command has been ran just once, when the delta table was first created.

# df_posts.write.format("delta").saveAsTable("jobposts_zaplatabg.posts")

# COMMAND ----------

# DBTITLE 1,Add Delta Table Constraint
# %sql

# ALTER TABLE jobposts_zaplatabg.posts ADD CONSTRAINT ZaplataBg_JobIdNotNull CHECK (JobId IS NOT NULL);

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM jobposts_zaplatabg.posts WHERE IsActive = True
# MAGIC -- DROP TABLE jobposts_zaplatabg.posts

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
deltaPosts = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Zaplata.bg/delta/posts")

targetDF = deltaPosts.toDF()
targetDF.display()

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
        (sourceDF.JobId == targetDF.JobId),
#         & (targetDF.IsActive == "true"),
        "outer"
    )
    .select(
        sourceDF["*"],
        targetDF.JobId.alias("target_JobId"),
        targetDF.JobLink.alias("target_JobLink"),
        targetDF.Company.alias("target_Company"),
        targetDF.JobTitle.alias("target_JobTitle"),
        targetDF.Location.alias("target_Location"),
        targetDF.MaxSalary.alias("target_MaxSalary"),
        targetDF.MinSalary.alias("target_MinSalary"),
        targetDF.DatePosted.alias("target_DatePosted"),
        targetDF.JobOfferType.alias("target_JobOfferType"),
        targetDF.Source.alias("target_Source"),
        targetDF.IngestionDate.alias("target_IngestionDate")
    )
)

joinDF.display()

# COMMAND ----------

# DBTITLE 1,Hash source and target columns and compare them
filterDF = joinDF.filter(xxhash64(*[col for col in joinDF.columns if col.startswith("target") == False and "IngestionDate" not in col]) != xxhash64(*[col for col in joinDF.columns if col.startswith("target") == True and "IngestionDate" not in col])).withColumn("MergeKey", col("target_JobId"))

filterDF.display()

# COMMAND ----------

# DBTITLE 1,Add MergeKey and set it to null where Id is not null
dummyDF = filterDF.filter(col("target_JobId").isNotNull()).withColumn("MergeKey", lit(None))

dummyDF.display()

# COMMAND ----------

# DBTITLE 1,Union DFs
scdDF = filterDF.union(dummyDF)

scdDF.display()

# COMMAND ----------

# DBTITLE 1,Create Dictionary which will be used in the Merge Command
columns_dict = {col: "source." + col for col in df_posts.columns}
columns_dict["IsActive"] = "'True'"
columns_dict["StartDate"] = "current_timestamp"
# columns_dict["EndDate"] = """to_date('9999-12-31 00:00:00.0000', 'MM-dd-yyyy HH:mm:ss')"""

columns_dict

# COMMAND ----------

targetDF.display()

# COMMAND ----------

# DBTITLE 1,Merge
(deltaPosts.alias("target")
 .merge(
     scdDF.alias("source"),
     "target.JobId = source.MergeKey"
 )
 .whenMatchedUpdate(set = 
    {
        "Source": "'Zaplata.bg'",
        "IsActive": "'False'", 
        "EndDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
    }
 )
 .whenNotMatchedInsert(
     condition = "source.JobId IS NOT NULL",
     values =
        {
             'JobLink': 'source.JobLink',
             'Company': 'source.Company',
             'JobTitle': 'source.JobTitle',
             'Location': 'source.Location',
             'MaxSalary': 'source.MaxSalary',
             'MinSalary': 'source.MinSalary',
             'DatePosted': 'source.DatePosted',
             'JobOfferType': 'source.JobOfferType',
             'Source': 'source.Source',
             'IngestionDate': 'source.IngestionDate',
             'JobId': 'source.JobId',
             'IsActive': "'True'",
             'StartDate': 'current_timestamp',
        }
 )
 .execute()
)

# COMMAND ----------

# DBTITLE 1,Check Delta Table History
deltaPosts.history().display()

# COMMAND ----------

# DBTITLE 1,Compare Delta Table records with records in the Source DataFrame
# Read delta table into DataFrame
deltaFinalPosts = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Zaplata.bg/delta/posts")
finalTargetDF = deltaFinalPosts.toDF()

# Raise error if there are records in the delta table (when filtered to show only active records), which do not exists in the source DataFrame
targetExceptSourceCount = finalTargetDF.where(col("IsActive") == True).select("JobId").exceptAll(sourceDF.select("JobId")).count()
targetEqualsSourceCount = finalTargetDF.where(col("IsActive") == True).count() == sourceDF.count()

if targetExceptSourceCount > 0 or targetEqualsSourceCount == False:
    raise Exception("There are records in source, which do not exist in target.")
