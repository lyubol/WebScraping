# Databricks notebook source
# MAGIC %run "lirkov/IT Job Boards/Main"

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import Window

# COMMAND ----------

# DBTITLE 1,Define variables
# Date variables 
current_year = date.today().year
current_month = "0" + str(date.today().month) if len(str(date.today().month)) == 1 else date.today().month
current_day = "0" + str(date.today().day) if len(str(date.today().day)) == 1 else date.today().day

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data

# COMMAND ----------

# DBTITLE 1,Read Base tables
df_posts_noblehire_raw = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Noblehire.io/base/posts/{current_year}/{current_month}/{current_day}/")

# COMMAND ----------

df_posts_noblehire_raw.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data

# COMMAND ----------

df_posts_noblehire_interm = (
    df_posts_noblehire_raw
    .select(
        col("companyId").alias("JunkId"),
        col("Source").alias("SourceSystem"),
        col("businessTraveling").alias("BusinessTraveling"),
        col("customerFacing").alias("CustomerFacing"),
        col("description").alias("Description"),
        col("fullyRemote").alias("FullyRemote"),
        col("homeOfficeDays").alias("HomeOfficeDays"),
        col("homeOfficePer").alias("HomeOfficePer"),
        col("jobType").alias("JobType"),
        col("mainDatabase").alias("MainDatabase"),
        col("offeringStock").alias("OfferingStock"),
        col("primaryLanguage").alias("PrimaryLanguage"),
        col("productDescription").alias("ProductDescription"),
        col("role").alias("Role"),
        col("salaryCurrency").alias("SalaryCurrency"),
        col("salaryMax").alias("SalaryMax"),
        col("salaryMin").alias("SalaryMin"),
        col("salaryPeriod").alias("SalaryPeriod"),
        col("secondaryLanguage").alias("SecondaryLanguage"),
        col("secondaryPlatform").alias("SecondaryPlatform"),
        col("seniority").alias("Seniority"),
        col("slug").alias("Slug"),
        col("teamLeadName").alias("TeamLeadName"),
        col("teamLeadRole").alias("TeamLeadRole"),
        col("teamSizeMax").alias("TeamSizeMax"),
        col("teamSizeMin").alias("TeamSizeMin"),
        col("title").alias("Title")
    )
)

# COMMAND ----------

surrogate_key_window = Window.orderBy(monotonically_increasing_id())

# Generate surrogate keys
df_posts_noblehire_enriched = df_posts_noblehire_interm.select("*").withColumn("JunkKey", row_number().over(surrogate_key_window))

# COMMAND ----------

# Reorder columns
df_posts_noblehire_enriched = df_posts_noblehire_enriched.select("JunkKey", "JunkId", "SourceSystem", *[c for c in df_posts_noblehire_enriched.columns if c not in ["JunkKey", "JunkId", "SourceSystem"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to ADLS

# COMMAND ----------

# DBTITLE 1,Create DimJunk
df_posts_noblehire_enriched.write.format("delta").mode("overwrite").option("path", "/mnt/adlslirkov/it-job-boards/Warehouse/DimJunk").saveAsTable("WAREHOUSE.DimJunk")
