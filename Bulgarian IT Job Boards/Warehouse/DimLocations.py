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

df_locations_0 = (
    df_company_noblehire_raw
    .select(
        col("company_locations_id_0").alias("LocationId"),
        col("Source").alias("SourceSystem"),
        col("company_locations_comment_0").alias("LocationComment"),
        col("company_locations_founded_0").alias("LocationFounded"),
        col("company_locations_teamSize_0").alias("LocationTeamSize"),
        col("company_locations_address_0_formatted_address").alias("LocationAddress"),
        col("company_locations_address_0_location_type").alias("LocationType"),
        col("company_locations_address_0_latitude").alias("Latitude"),
        col("company_locations_address_0_longitude").alias("Longitude")
    )
    .where(col("company_locations_id_0").isNotNull())
)

df_locations_1 = (
    df_company_noblehire_raw
    .select(
        col("company_locations_id_1").alias("LocationId"),
        col("Source").alias("SourceSystem"),
        col("company_locations_comment_1").alias("LocationComment"),
        col("company_locations_founded_1").alias("LocationFounded"),
        col("company_locations_teamSize_1").alias("LocationTeamSize"),
        col("company_locations_address_1_formatted_address").alias("LocationAddress"),
        col("company_locations_address_1_location_type").alias("LocationType"),
        col("company_locations_address_1_latitude").alias("Latitude"),
        col("company_locations_address_1_longitude").alias("Longitude")
    )
    .where(col("company_locations_id_1").isNotNull())
)

df_locations_2 = (
    df_company_noblehire_raw
    .select(
        col("company_locations_id_2").alias("LocationId"),
        col("Source").alias("SourceSystem"),
        col("company_locations_comment_2").alias("LocationComment"),
        col("company_locations_founded_2").alias("LocationFounded"),
        col("company_locations_teamSize_2").alias("LocationTeamSize"),
        col("company_locations_address_2_formatted_address").alias("LocationAddress"),
        col("company_locations_address_2_location_type").alias("LocationType"),
        col("company_locations_address_2_latitude").alias("Latitude"),
        col("company_locations_address_2_longitude").alias("Longitude")
    )
    .where(col("company_locations_id_2").isNotNull())
)

df_locations_3 = (
    df_company_noblehire_raw
    .select(
        col("company_locations_id_3").alias("LocationId"),
        col("Source").alias("SourceSystem"),
        col("company_locations_comment_3").alias("LocationComment"),
        col("company_locations_founded_3").alias("LocationFounded"),
        col("company_locations_teamSize_3").alias("LocationTeamSize"),
        col("company_locations_address_3_formatted_address").alias("LocationAddress"),
        col("company_locations_address_3_location_type").alias("LocationType"),
        col("company_locations_address_3_latitude").alias("Latitude"),
        col("company_locations_address_3_longitude").alias("Longitude")
    )
    .where(col("company_locations_id_3").isNotNull())
)

df_locations_4 = (
    df_company_noblehire_raw
    .select(
        col("company_locations_id_4").alias("LocationId"),
        col("Source").alias("SourceSystem"),
        col("company_locations_comment_4").alias("LocationComment"),
        col("company_locations_founded_4").alias("LocationFounded"),
        col("company_locations_teamSize_4").alias("LocationTeamSize"),
        col("company_locations_address_4_formatted_address").alias("LocationAddress"),
        col("company_locations_address_4_location_type").alias("LocationType"),
        col("company_locations_address_4_latitude").alias("Latitude"),
        col("company_locations_address_4_longitude").alias("Longitude")
    )
    .where(col("company_locations_id_4").isNotNull())
)

df_locations_union = df_locations_0.union(df_locations_1).union(df_locations_2).union(df_locations_3).union(df_locations_4)

# COMMAND ----------

surrogate_key_window = Window.orderBy(monotonically_increasing_id())

# Generate surrogate keys
df_locations_union_enriched = df_locations_union.select("LocationId", "SourceSystem", "LocationComment", "LocationFounded", "LocationTeamSize", "LocationAddress", "LocationType", "Latitude", "Longitude").withColumn("LocationKey", row_number().over(surrogate_key_window))

# COMMAND ----------

# Reorder columns
df_locations_union_enriched = df_locations_union_enriched.select("LocationKey", "LocationId", "SourceSystem", "LocationComment", "LocationFounded", "LocationTeamSize", "LocationAddress", "LocationType", "Latitude", "Longitude")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to ADLS

# COMMAND ----------

# DBTITLE 1,Create DimLocations
df_locations_union_enriched.write.format("delta").mode("overwrite").option("path", "/mnt/adlslirkov/it-job-boards/Warehouse/DimLocations").saveAsTable("WAREHOUSE.DimLocations")
