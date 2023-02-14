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
df_values_noblehire_raw = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Noblehire.io/base/companyValues/{current_year}/{current_month}/{current_day}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data

# COMMAND ----------

surrogate_key_window = Window.orderBy(monotonically_increasing_id())

# Generate surrogate keys
df_values_noblehire_enriched = df_values_noblehire_raw.select(col("companyId").alias("ValuesId"), col("Source").alias("SourceSystem"), *[f"company_values_title_{c}" for c in range(0, 8)], *[f"company_values_text_{c}" for c in range(0, 8)]).withColumn("ValuesKey", row_number().over(surrogate_key_window))

# COMMAND ----------

# Rename columns
df_values_noblehire_enriched = (
    df_values_noblehire_enriched
    .toDF("ValuesId", "SourceSystem", *[c.replace("company_", "").replace("_text", "Text").replace("_title", "Title").replace("_", "") for c in df_values_noblehire_enriched.columns if c.startswith("company_")], "ValuesKey")
)

# Reorder columns
df_values_noblehire_enriched = df_values_noblehire_enriched.select("ValuesKey", "ValuesId", "SourceSystem", *[c for c in df_values_noblehire_enriched.columns if c not in ["ValuesId", "ValuesKey", "SourceSystem"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to ADLS

# COMMAND ----------

# DBTITLE 1,Create DimValues
df_values_noblehire_enriched.write.format("delta").mode("overwrite").option("path", "/mnt/adlslirkov/it-job-boards/Warehouse/DimValues").saveAsTable("WAREHOUSE.DimValues")
