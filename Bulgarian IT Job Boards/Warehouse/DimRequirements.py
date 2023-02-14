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
df_req_noblehire_raw = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Noblehire.io/base/jobRequirements/{current_year}/{current_month}/{current_day}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data

# COMMAND ----------

surrogate_key_window = Window.orderBy(monotonically_increasing_id())

# Generate surrogate keys
df_req_noblehire_enriched = df_req_noblehire_raw.select(col("id").alias("RequirementsId"), col("Source").alias("SourceSystem"), *[f"requirements_{c}_title" for c in range(0, 23)]).withColumn("RequirementsKey", row_number().over(surrogate_key_window))

# COMMAND ----------

# Rename columns
df_req_noblehire_enriched = (
    df_req_noblehire_enriched
    .toDF("RequirementsId", "SourceSystem", *[c.replace("_title", "").replace("_", "").title() for c in df_req_noblehire_enriched.columns if c.endswith("_title")], "RequirementsKey")
)

# Reorder columns
df_req_noblehire_enriched = df_req_noblehire_enriched.select("RequirementsKey", "RequirementsId", "SourceSystem", *[c for c in df_req_noblehire_enriched.columns if c not in ["RequirementsId", "RequirementsKey", "SourceSystem"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to ADLS

# COMMAND ----------

# DBTITLE 1,Create DimRequirements
df_req_noblehire_enriched.write.format("delta").mode("overwrite").option("path", "/mnt/adlslirkov/it-job-boards/Warehouse/DimRequirements").saveAsTable("WAREHOUSE.DimRequirements")
