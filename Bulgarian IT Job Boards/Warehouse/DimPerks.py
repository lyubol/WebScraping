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
df_perks_noblehire_raw = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Noblehire.io/base/companyPerks/{current_year}/{current_month}/{current_day}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data

# COMMAND ----------

surrogate_key_window = Window.orderBy(monotonically_increasing_id())

# Generate surrogate keys
df_perks_noblehire_enriched = df_perks_noblehire_raw.select(col("companyId").alias("PerksId"), col("Source").alias("SourceSystem"), *[f"company_perks_text_{c}" for c in range(0, 15)]).withColumn("PerksKey", row_number().over(surrogate_key_window))

# COMMAND ----------

# Rename columns
df_perks_noblehire_enriched = (
    df_perks_noblehire_enriched
    .toDF("PerksId", "SourceSystem", *[c.replace("company_", "").replace("_text", "").replace("_", "").title() for c in df_perks_noblehire_enriched.columns if c.startswith("company_")], "PerksKey")
)

# Reorder columns
df_perks_noblehire_enriched = df_perks_noblehire_enriched.select("PerksKey", "PerksId", "SourceSystem", *[c for c in df_perks_noblehire_enriched.columns if c not in ["PerksId", "PerksKey", "SourceSystem"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to ADLS

# COMMAND ----------

# DBTITLE 1,Create DimPerks
df_perks_noblehire_enriched.write.format("delta").mode("overwrite").option("path", "/mnt/adlslirkov/it-job-boards/Warehouse/DimPerks").saveAsTable("WAREHOUSE.DimPerks")
