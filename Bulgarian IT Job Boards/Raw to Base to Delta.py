# Databricks notebook source
from datetime import date
import time
from pyspark.sql.functions import sha2, concat, length, current_timestamp, to_date, lit, regexp_replace, when, col

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Raw to Base

# COMMAND ----------

# DBTITLE 1,Define variables
# Target location paths
raw_main_path = "/mnt/adlslirkov/it-job-boards/DEV.bg/raw/"
base_main_path = "/mnt/adlslirkov/it-job-boards/DEV.bg/base/"
posts_path = f"posts/{date.today().year}/{date.today().month}/{date.today().day}/"
posts_file_name = f"posts-{date.today()}.csv"

# COMMAND ----------

# DBTITLE 1,Apply Raw to Base transformations
df_posts = spark.read.format("csv").option("header", True).load(raw_main_path + posts_path + posts_file_name)

# Rename columns
df_posts = (
    df_posts
    .withColumnRenamed("title", "Title")
    .withColumnRenamed("company", "Company")
    .withColumnRenamed("location", "Location")
    .withColumnRenamed("uploaded", "Uploaded")
    .withColumnRenamed("salary", "Salary")
    .withColumnRenamed("department", "Department")
    .withColumnRenamed("link", "Link")
)

# Add date and source column
df_posts = (
    df_posts
    .withColumn("IngestionDate", to_date(current_timestamp()))
    .withColumn("Source", lit("DEV.bg"))
)

# Add hash column using sha2, to act as unique identifier
df_posts = (
    df_posts
    .withColumn(
        "Id", 
        sha2(
            concat(
                "Source", 
                "Title", 
                "Company", 
                "Location", 
                "Uploaded", 
                "Salary", 
                "Department", 
                "Link"
            ), 
            256)
    )
)

# Drop and reorder columns
df_posts = (
    df_posts
    .drop("_c0")
    .select(
        "Id", 
        "IngestionDate", 
        "Source", 
        "Title", 
        "Company", 
        "Location", 
        "Uploaded", 
        "Salary", 
        "Department", 
        "Link")
)

# Make sure the id column is unique
distinct_count = df_posts.select("Id").distinct().count()
count_all = df_posts.select("Id").count()
try:
    assert distinct_count == count_all
except AssertionError:
    print("Id column contains duplicates")
    raise 

# COMMAND ----------

# DBTITLE 1,Write to ADLS (Base)
df_posts.write.format("parquet").save(base_main_path + posts_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Base to Delta

# COMMAND ----------

# DBTITLE 1,Create temp staging Delta table
df_posts.write.saveAsTable("jobposts.stage_posts")

# COMMAND ----------

# DBTITLE 1,Update Posts Delta table
# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO jobposts.posts AS posts
# MAGIC USING jobposts.stage_posts AS stage_posts
# MAGIC ON posts.Id = stage_posts.Id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET 
# MAGIC     Id = stage_posts.Id,
# MAGIC     IngestionDate = stage_posts.IngestionDate,
# MAGIC     Source = stage_posts.Source,
# MAGIC     Title = stage_posts.Title,
# MAGIC     Company = stage_posts.Company,
# MAGIC     Location = stage_posts.Location,
# MAGIC     Uploaded = stage_posts.Uploaded,
# MAGIC     Salary = stage_posts.Salary,
# MAGIC     Department = stage_posts.Department,
# MAGIC     Link = stage_posts.Link
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT(
# MAGIC     Id, 
# MAGIC     IngestionDate, 
# MAGIC     Source, 
# MAGIC     Title, 
# MAGIC     Company,
# MAGIC     Location,
# MAGIC     Uploaded,
# MAGIC     Salary,
# MAGIC     Department,
# MAGIC     Link,
# MAGIC     IsActive
# MAGIC   ) 
# MAGIC   VALUES(
# MAGIC     stage_posts.Id,
# MAGIC     stage_posts.IngestionDate,
# MAGIC     stage_posts.Source,
# MAGIC     stage_posts.Title,
# MAGIC     stage_posts.Company,
# MAGIC     stage_posts.Location,
# MAGIC     stage_posts.Uploaded,
# MAGIC     stage_posts.Salary,
# MAGIC     stage_posts.Department,
# MAGIC     stage_posts.Link,
# MAGIC     1
# MAGIC   );

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC UPDATE jobposts.posts
# MAGIC SET IsActive = 0
# MAGIC WHERE Id IN (
# MAGIC   SELECT Id FROM jobposts.posts
# MAGIC   EXCEPT
# MAGIC   SELECT Id FROM jobposts.stage_posts
# MAGIC   )

# COMMAND ----------

# DBTITLE 1,Drop staging Delta table
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE jobposts.stage_posts 
