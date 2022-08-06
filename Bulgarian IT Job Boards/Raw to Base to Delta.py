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
spark.sql("""
    MERGE INTO jobposts.posts AS posts
    USING jobposts.stage_posts AS stage_posts
    ON posts.Id = stage_posts.Id
    WHEN MATCHED THEN
      UPDATE SET 
        Id = stage_posts.Id,
        IngestionDate = stage_posts.IngestionDate,
        Source = stage_posts.Source,
        Title = stage_posts.Title,
        Company = stage_posts.Company,
        Location = stage_posts.Location,
        Uploaded = stage_posts.Uploaded,
        Salary = stage_posts.Salary,
        Department = stage_posts.Department,
        Link = stage_posts.Link
    WHEN NOT MATCHED THEN 
      INSERT(
        Id, 
        IngestionDate, 
        Source, 
        Title, 
        Company,
        Location,
        Uploaded,
        Salary,
        Department,
        Link,
        IsActive
      ) 
      VALUES(
        stage_posts.Id,
        stage_posts.IngestionDate,
        stage_posts.Source,
        stage_posts.Title,
        stage_posts.Company,
        stage_posts.Location,
        stage_posts.Uploaded,
        stage_posts.Salary,
        stage_posts.Department,
        stage_posts.Link,
        1
      );
""")

# COMMAND ----------

spark.sql("""
    UPDATE jobposts.posts
    SET IsActive = 0
    WHERE Id IN (
      SELECT Id FROM jobposts.posts
      EXCEPT
      SELECT Id FROM jobposts.stage_posts
      );
""")



# COMMAND ----------

# DBTITLE 1,Drop staging Delta table
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE jobposts.stage_posts 
