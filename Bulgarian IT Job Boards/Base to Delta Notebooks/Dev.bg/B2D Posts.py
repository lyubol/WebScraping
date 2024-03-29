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
main_path = "/mnt/adlslirkov/it-job-boards/DEV.bg/base/"
posts_path = f"posts/{current_year}/{current_month}/{current_day}/"

# COMMAND ----------

# DBTITLE 1,Create Temp Staging Table
# Read Base
df_jobposts = spark.read.format("parquet").load(main_path + posts_path)

# Create the Source Data Frame
sourceDF = df_jobposts
sourceDF.display()
print("Count: {}".format(sourceDF.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Database and Tables

# COMMAND ----------

# DBTITLE 1,Create Database
# MAGIC %sql
# MAGIC 
# MAGIC -- CREATE DATABASE IF NOT EXISTS jobposts_devbg
# MAGIC -- COMMENT 'This database holds job posts data coming from Dev.bg'
# MAGIC -- LOCATION '/mnt/adlslirkov/it-job-boards/DEV.bg/delta/'

# COMMAND ----------

# DBTITLE 1,Add SCD Type 2 Columns to Delta Table
# # This command has been ran just once, when the delta table was first created.

# df_jobposts = (
#     df_jobposts
#     .withColumn("IsActive", lit(True))
#     .withColumn("StartDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
#     .withColumn("EndDate", lit(None).cast(StringType()))
# )

# COMMAND ----------

# DBTITLE 1,Create Delta Table
# # This command has been ran just once, when the delta table was first created.

# df_jobposts.write.format("delta").saveAsTable("jobposts_devbg.posts")

# COMMAND ----------

# DBTITLE 1,Add Delta Table Constraint
# MAGIC %sql
# MAGIC 
# MAGIC -- ALTER TABLE jobposts_devbg.posts ADD CONSTRAINT LinkAndDepartmentNotNull CHECK (Link IS NOT NULL AND Department IS NOT NULL);

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM jobposts_devbg.posts WHERE IsActive = True
# MAGIC -- DROP TABLE jobposts_devbg.posts

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
deltaPosts = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/DEV.bg/delta/posts")

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
        (sourceDF.Link == targetDF.Link)
        & (sourceDF.Department == targetDF.Department),
#         & (targetDF.IsActive == "true"),
        "outer"
    )
    .select(
        sourceDF["*"],
        targetDF.Company.alias("target_Company"),
        targetDF.Department.alias("target_Department"),
        targetDF.Link.alias("target_Link"),
        targetDF.Location.alias("target_Location"),
        targetDF.Salary.alias("target_Salary"),
        targetDF.Title.alias("target_Title"),
        targetDF.Uploaded.alias("target_Uploaded"),
        targetDF.Source.alias("target_Source"),
        targetDF.IngestionDate.alias("target_IngestionDate")
    )
)

joinDF.display()

# COMMAND ----------

# DBTITLE 1,Hash source and target columns and compare them
filterDF = joinDF.filter(xxhash64("Company", "Department", "Link", "Location", "Salary", "Title", "Uploaded", "Source") != xxhash64("target_Company", "target_Department", "target_Link", "target_Location", "target_Salary", "target_Title", "target_Uploaded", "target_Source")).withColumn("MergeKey", concat("target_Link", "target_Department"))

filterDF.display()

# COMMAND ----------

# DBTITLE 1,Add MergeKey and set it to null where Id is not null
dummyDF = filterDF.filter((col("target_Link").isNotNull()) & (col("target_Department").isNotNull())).withColumn("MergeKey", lit(None))

dummyDF.display()

# COMMAND ----------

# DBTITLE 1,Union DFs
scdDF = filterDF.union(dummyDF)

scdDF.display()

# COMMAND ----------

# DBTITLE 1,Create Dictionary which will be used in the Merge Command
columns_dict = {col: "source." + col for col in df_jobposts.columns}
columns_dict["IsActive"] = "'True'"
columns_dict["StartDate"] = "current_timestamp"
# columns_dict["EndDate"] = """to_date('9999-12-31 00:00:00.0000', 'MM-dd-yyyy HH:mm:ss')"""

columns_dict

# COMMAND ----------

# DBTITLE 1,Merge
(deltaPosts.alias("target")
 .merge(
     scdDF.alias("source"),
     condition = "concat(target.Link, target.Department) = source.MergeKey"
 )
 .whenMatchedUpdate(set = 
    {
        "Source": "'Dev.bg'",
        "IsActive": "'False'", 
        "EndDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
    }
 )
 .whenNotMatchedInsert(
     condition = "source.Link IS NOT NULL AND source.Department IS NOT NULL",
     values =
        {
             'company': 'source.Company',
             'department': 'source.Department',
             'link': 'source.Link',
             'location': 'source.Location',
             'salary': 'source.Salary',
             'title': 'source.Title',
             'uploaded': 'source.Uploaded',
             'Source': "'Dev.bg'",
             'IngestionDate': 'source.IngestionDate',
             'IsActive': "'True'",
             'StartDate': 'current_timestamp'
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
deltaFinalPosts = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/DEV.bg/delta/posts")
finalTargetDF = deltaFinalPosts.toDF()

# Raise error if there are records in the delta table (when filtered to show only active records), which do not exists in the source DataFrame
targetExceptSourceCount = finalTargetDF.where(col("IsActive") == True).select(concat("Link", "Department")).exceptAll(sourceDF.select(concat("Link", "Department"))).count()
targetEqualsSourceCount = finalTargetDF.where(col("IsActive") == True).count() == sourceDF.count()

if targetExceptSourceCount > 0 or targetEqualsSourceCount == False:
    raise Exception("There are records in source, which do not exist in target.")
