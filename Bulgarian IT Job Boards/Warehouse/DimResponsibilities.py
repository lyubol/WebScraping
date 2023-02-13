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
df_resp_noblehire_raw = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Noblehire.io/base/jobResponsibilities/{current_year}/{current_month}/{current_day}/")

df_dim_source_systems = spark.read.format("delta").load("/mnt/adlslirkov/it-job-boards/Warehouse/DimSourceSystems")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data

# COMMAND ----------

surrogate_key_window = Window.orderBy(monotonically_increasing_id())

# Generate surrogate keys
df_resp_noblehire_enriched = df_resp_noblehire_raw.select(col("id").alias("responsibilities_id"), *[f"responsibilities_{c}_title" for c in range(1, 18)]).withColumn("ResponsibilitiesKey", row_number().over(surrogate_key_window))

# COMMAND ----------

df_resp_noblehire_enriched.display()

# COMMAND ----------

[c.replace("_title", "").replace("_", "").title() for c in df_resp_noblehire_enriched.columns if c.endswith("_title")]

# COMMAND ----------

# Add source system column and rename columns
df_resp_noblehire_enriched = (
    df_resp_noblehire_enriched
    .withColumn("SourceSystem", lit("Noblehire.io"))
    .withColumnRenamed("responsibilities_id", "ResponsibilitiesId")
    .toDF("ResponsibilitiesId", *[c.replace("_title", "").replace("_", "").title() for c in df_resp_noblehire_enriched.columns if c.endswith("_title")], "ResponsibilitiesKey", "SourceSystem")
)

# Reorder columns
df_resp_noblehire_enriched = df_resp_noblehire_enriched.select("ResponsibilitiesKey", "ResponsibilitiesId", "SourceSystem", *[c for c in df_resp_noblehire_enriched.columns if c not in ["ResponsibilitiesId", "ResponsibilitiesKey", "SourceSystem"]])

# COMMAND ----------

# DBTITLE 1,Create DimResponsibilities
df_resp_noblehire_enriched.write.format("delta").mode("overwrite").option("path", "/mnt/adlslirkov/it-job-boards/Warehouse/DimResponsibilities").saveAsTable("WAREHOUSE.DimResponsibilities")
