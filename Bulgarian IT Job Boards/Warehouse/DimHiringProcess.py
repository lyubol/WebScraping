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
df_hiring_process_noblehire_raw = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Noblehire.io/base/jobHiringProcess/{current_year}/{current_month}/{current_day}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data

# COMMAND ----------

surrogate_key_window = Window.orderBy(monotonically_increasing_id())

# Generate surrogate keys
df_hiring_process_noblehire_enriched = df_hiring_process_noblehire_raw.select(col("id").alias("HiringProcessId"), col("Source").alias("SourceSystem"), *[f"hiringProcessSteps_{c}" for c in range(0, 6)]).withColumn("HiringProcessKey", row_number().over(surrogate_key_window))

# COMMAND ----------

# Rename columns
df_hiring_process_noblehire_enriched = (
    df_hiring_process_noblehire_enriched
    .toDF("HiringProcessId", "SourceSystem", *[c.replace("_", "").replace("hiring", "Hiring") for c in df_hiring_process_noblehire_enriched.columns if c.startswith("hiring")], "HiringProcessKey")
)

# Reorder columns
df_hiring_process_noblehire_enriched = df_hiring_process_noblehire_enriched.select("HiringProcessKey", "HiringProcessId", "SourceSystem", *[c for c in df_hiring_process_noblehire_enriched.columns if c not in ["HiringProcessId", "HiringProcessKey", "SourceSystem"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to ADLS

# COMMAND ----------

# DBTITLE 1,Create DimHiringProcess
df_hiring_process_noblehire_enriched.write.format("delta").mode("overwrite").option("path", "/mnt/adlslirkov/it-job-boards/Warehouse/DimHiringProcess").saveAsTable("WAREHOUSE.DimHiringProcess")
