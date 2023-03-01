# Databricks notebook source
# MAGIC %run "lirkov/IT Job Boards/Main"

# COMMAND ----------

# DBTITLE 1,Imports
import pyspark.sql.utils
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
df_company_noblehire_raw = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Noblehire.io/base/companyLocations/{current_year}/{current_month}/{current_day}/")

df_company_devbg = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/DEV.bg/base/company/{current_year}/{current_month}/{current_day}/")

# df_company_zaplata = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Zaplata.bg/base/posts/{current_year}/{current_month}/{current_day}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data

# COMMAND ----------

df_locations_union_enriched = (df_company_noblehire_raw
      .withColumn("locations_4_teamSize", lit(None))
      .select(
        col("id").alias("JobPostId"),
        col("companyId").alias("CompanyId"),
        col("Source").alias("SourceSystem"),
        col("locations_0_id").alias("LocationId_0"),
        col("locations_0_comment").alias("LocationComment_0"),
        col("locations_0_founded").alias("LocationFounded_0"),
        col("locations_0_teamSize").alias("LocationTeamSize_0"),
        col("locations_0_address_formatted_address").alias("LocationAddress_0"),
        col("locations_0_address_location_type").alias("LocationType_0"),
        col("locations_0_address_latitude").alias("Latitude_0"),
        col("locations_0_address_longitude").alias("Longitude_0"),
        col("locations_1_id").alias("LocationId_1"),
        col("locations_1_comment").alias("LocationComment_1"),
        col("locations_1_founded").alias("LocationFounded_1"),
        col("locations_1_teamSize").alias("LocationTeamSize_1"),
        col("locations_1_address_formatted_address").alias("LocationAddress_1"),
        col("locations_1_address_location_type").alias("LocationType_1"),
        col("locations_1_address_latitude").alias("Latitude_1"),
        col("locations_1_address_longitude").alias("Longitude_1"),
        col("locations_2_id").alias("LocationId_2"),
        col("locations_2_comment").alias("LocationComment_2"),
        col("locations_2_founded").alias("LocationFounded_2"),
        col("locations_2_teamSize").alias("LocationTeamSize_2"),
        col("locations_2_address_formatted_address").alias("LocationAddress_2"),
        col("locations_2_address_location_type").alias("LocationType_2"),
        col("locations_2_address_latitude").alias("Latitude_2"),
        col("locations_2_address_longitude").alias("Longitude_2"),
        col("locations_3_id").alias("LocationId_3"),
        col("locations_3_comment").alias("LocationComment_3"),
        col("locations_3_founded").alias("LocationFounded_3"),
        col("locations_3_teamSize").alias("LocationTeamSize_3"),
        col("locations_3_address_formatted_address").alias("LocationAddress_3"),
        col("locations_3_address_location_type").alias("LocationType_3"),
        col("locations_3_address_latitude").alias("Latitude_3"),
        col("locations_3_address_longitude").alias("Longitude_3"),
        col("locations_4_id").alias("LocationId_4"),
        col("locations_4_comment").alias("LocationComment_4"),
        col("locations_4_founded").alias("LocationFounded_4"),
        col("locations_4_teamSize").alias("LocationTeamSize_4"),
        col("locations_4_address_formatted_address").alias("LocationAddress_4"),
        col("locations_4_address_location_type").alias("LocationType_4"),
        col("locations_4_address_latitude").alias("Latitude_4"),
        col("locations_4_address_longitude").alias("Longitude_4")
      )
)

# COMMAND ----------

# df_locations_0 = (
#     df_company_noblehire_raw
#     .select(
#         col("locations_0_id").alias("LocationId"),
#         col("id").alias("JobPostId"),
#         col("companyId").alias("CompanyId"),
#         col("Source").alias("SourceSystem"),
#         col("locations_0_comment").alias("LocationComment"),
#         col("locations_0_founded").alias("LocationFounded"),
#         col("locations_0_teamSize").alias("LocationTeamSize"),
#         col("locations_0_address_formatted_address").alias("LocationAddress"),
#         col("locations_0_address_location_type").alias("LocationType"),
#         col("locations_0_address_latitude").alias("Latitude"),
#         col("locations_0_address_longitude").alias("Longitude")
#     )
#     .where(col("locations_0_id").isNotNull())
# )

# df_locations_1 = (
#     df_company_noblehire_raw
#     .select(
#         col("locations_1_id").alias("LocationId"),
#         col("id").alias("JobPostId"),
#         col("companyId").alias("CompanyId"),
#         col("Source").alias("SourceSystem"),
#         col("locations_1_comment").alias("LocationComment"),
#         col("locations_1_founded").alias("LocationFounded"),
#         col("locations_1_teamSize").alias("LocationTeamSize"),
#         col("locations_1_address_formatted_address").alias("LocationAddress"),
#         col("locations_1_address_location_type").alias("LocationType"),
#         col("locations_1_address_latitude").alias("Latitude"),
#         col("locations_1_address_longitude").alias("Longitude")
#     )
#     .where(col("locations_1_id").isNotNull())
# )

# df_locations_2 = (
#     df_company_noblehire_raw
#     .select(
#         col("locations_2_id").alias("LocationId"),
#         col("id").alias("JobPostId"),
#         col("companyId").alias("CompanyId"),
#         col("Source").alias("SourceSystem"),
#         col("locations_2_comment").alias("LocationComment"),
#         col("locations_2_founded").alias("LocationFounded"),
#         col("locations_2_teamSize").alias("LocationTeamSize"),
#         col("locations_2_address_formatted_address").alias("LocationAddress"),
#         col("locations_2_address_location_type").alias("LocationType"),
#         col("locations_2_address_latitude").alias("Latitude"),
#         col("locations_2_address_longitude").alias("Longitude")
#     )
#     .where(col("locations_2_id").isNotNull())
# )

# df_locations_3 = (
#     df_company_noblehire_raw
#     .select(
#         col("locations_3_id").alias("LocationId"),
#         col("id").alias("JobPostId"),
#         col("companyId").alias("CompanyId"),
#         col("Source").alias("SourceSystem"),
#         col("locations_3_comment").alias("LocationComment"),
#         col("locations_3_founded").alias("LocationFounded"),
#         col("locations_3_teamSize").alias("LocationTeamSize"),
#         col("locations_3_address_formatted_address").alias("LocationAddress"),
#         col("locations_3_address_location_type").alias("LocationType"),
#         col("locations_3_address_latitude").alias("Latitude"),
#         col("locations_3_address_longitude").alias("Longitude")
#     )
#     .where(col("locations_3_id").isNotNull())
# )

# try:
#     df_locations_4 = (
#         df_company_noblehire_raw
#         .select(
#             col("locations_4_id").alias("LocationId"),
#             col("id").alias("JobPostId"),
#             col("companyId").alias("CompanyId"),
#             col("Source").alias("SourceSystem"),
#             col("locations_4_comment").alias("LocationComment"),
#             col("locations_4_founded").alias("LocationFounded"),
#             col("locations_4_teamSize").alias("LocationTeamSize"),
#             col("locations_4_address_formatted_address").alias("LocationAddress"),
#             col("locations_4_address_location_type").alias("LocationType"),
#             col("locations_4_address_latitude").alias("Latitude"),
#             col("locations_4_address_longitude").alias("Longitude")
#         )
#         .where(col("locations_4_id").isNotNull()))
# except pyspark.sql.utils.AnalysisException:
#     df_locations_4 = (
#         df_company_noblehire_raw
#         .withColumn("locations_4_teamSize", lit(None))
#         .select(
#             col("locations_4_id").alias("LocationId"),
#             col("id").alias("JobPostId"),
#             col("companyId").alias("CompanyId"),
#             col("Source").alias("SourceSystem"),
#             col("locations_4_comment").alias("LocationComment"),
#             col("locations_4_founded").alias("LocationFounded"),
#             col("locations_4_teamSize").alias("LocationTeamSize"),
#             col("locations_4_address_formatted_address").alias("LocationAddress"),
#             col("locations_4_address_location_type").alias("LocationType"),
#             col("locations_4_address_latitude").alias("Latitude"),
#             col("locations_4_address_longitude").alias("Longitude")
#         )
#         .where(col("locations_4_id").isNotNull()))


# df_locations_union = df_locations_0.union(df_locations_1).union(df_locations_2).union(df_locations_3).union(df_locations_4)

# COMMAND ----------

surrogate_key_window = Window.orderBy(monotonically_increasing_id())

# Generate surrogate keys
df_locations_union_enriched = (
    df_locations_union_enriched
    .withColumn("LocationKey", row_number().over(surrogate_key_window))
    .select("LocationKey", "*")
)

# COMMAND ----------

# Create a function to extract the address in format - City, Country when the LocationType is in:
# ROOFTOP, RANGE_INTERPOLATED, GEOMETRIC_CENTER

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to ADLS

# COMMAND ----------

# DBTITLE 1,Create DimLocations
# df_locations_union_enriched_final.write.format("delta").mode("overwrite").option("path", "/mnt/adlslirkov/it-job-boards/Warehouse/DimLocations").saveAsTable("WAREHOUSE.DimLocations")
