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
df_company_noblehire_raw = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Noblehire.io/base/companyGeneral/{current_year}/{current_month}/{current_day}/")

df_company_devbg = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/DEV.bg/base/company/{current_year}/{current_month}/{current_day}/")

df_company_zaplata = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Zaplata.bg/base/posts/{current_year}/{current_month}/{current_day}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data

# COMMAND ----------

df_company_noblehire_interm = (
    df_company_noblehire_raw
    .select(
        col("companyId").alias("CompanyId"),
        col("brand").alias("CompanyName"),
        col("Source").alias("SourceSystem"),
        col("overview").alias("Overview"),
        col("product").alias("Product"),
        col("public").alias("IsPublic"),
        col("slug").alias("CompanySlug")
    )
)

# COMMAND ----------

surrogate_key_window = Window.orderBy(monotonically_increasing_id())

# Generate surrogate keys
df_company_noblehire_enriched = df_company_noblehire_interm.select("CompanyId", "SourceSystem", "CompanyName", "Overview", "Product", "IsPublic", "CompanySlug").withColumn("CompanyKey", row_number().over(surrogate_key_window))

# COMMAND ----------

# Reorder columns
df_company_noblehire_enriched = df_company_noblehire_enriched.select("CompanyKey", "CompanyId", "SourceSystem", "CompanyName", "Overview", "Product", "IsPublic", "CompanySlug")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to ADLS

# COMMAND ----------

# DBTITLE 1,Create DimCompany
df_company_noblehire_enriched.write.format("delta").mode("overwrite").option("path", "/mnt/adlslirkov/it-job-boards/Warehouse/DimCompany").saveAsTable("WAREHOUSE.DimCompany")
