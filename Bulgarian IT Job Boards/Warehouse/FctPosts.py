# Databricks notebook source
# MAGIC %run "lirkov/IT Job Boards/Main"

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
df_posts_noblehire = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Noblehire.io/base/posts/{current_year}/{current_month}/{current_day}/")

# df_posts_devbg = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/DEV.bg/base/posts/{current_year}/{current_month}/{current_day}/")

# df_posts_zaplata = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Zaplata.bg/base/posts/{current_year}/{current_month}/{current_day}/")

# COMMAND ----------

# DBTITLE 1,Read Dimensions
df_dim_date = spark.read.format("delta").load("/mnt/adlslirkov/it-job-boards/Warehouse/DimDate/")

df_dim_source_systems = spark.read.format("delta").load("/mnt/adlslirkov/it-job-boards/Warehouse/DimSourceSystems/")

df_dim_activities = spark.read.format("delta").load("/mnt/adlslirkov/it-job-boards/Warehouse/DimActivities/")

df_dim_awards = spark.read.format("delta").load("/mnt/adlslirkov/it-job-boards/Warehouse/DimAwards/")

df_dim_benefits = spark.read.format("delta").load("/mnt/adlslirkov/it-job-boards/Warehouse/DimBenefits/")

df_dim_company = spark.read.format("delta").load("/mnt/adlslirkov/it-job-boards/Warehouse/DimCompany/")

df_dim_hiring_process = spark.read.format("delta").load("/mnt/adlslirkov/it-job-boards/Warehouse/DimHiringProcess/")

df_dim_locations = spark.read.format("delta").load("/mnt/adlslirkov/it-job-boards/Warehouse/DimLocations/")

df_dim_perks = spark.read.format("delta").load("/mnt/adlslirkov/it-job-boards/Warehouse/DimPerks/")

df_dim_requirements = spark.read.format("delta").load("/mnt/adlslirkov/it-job-boards/Warehouse/DimRequirements/")

df_dim_responsibilities = spark.read.format("delta").load("/mnt/adlslirkov/it-job-boards/Warehouse/DimResponsibilities/")

df_dim_tools = spark.read.format("delta").load("/mnt/adlslirkov/it-job-boards/Warehouse/DimTools/")

df_dim_values = spark.read.format("delta").load("/mnt/adlslirkov/it-job-boards/Warehouse/DimValues/")

df_dim_junk = spark.read.format("delta").load("/mnt/adlslirkov/it-job-boards/Warehouse/DimJunk/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data

# COMMAND ----------

# DBTITLE 1,Prepare DimDate
# Get all dates between today's date and eleven months ago
df_date_filtered = (df_dim_date
 .where(
     (col("CalendarDate") < current_date()) & 
     (col("CalendarDate") > add_months(current_date(), -11))
 )
)

# Add columns to match the dates in DevBg and ZaplataBg, so we can join them with the dates table
df_date_filtered = (df_date_filtered
 .withColumn("DevbgMonth", 
     when(col("CalendarMonth") == "January", "ян")
     .when(col("CalendarMonth") == "February", "фев")
     .when(col("CalendarMonth") == "March", "мар")
     .when(col("CalendarMonth") == "April", "апр")
     .when(col("CalendarMonth") == "May", "май")
     .when(col("CalendarMonth") == "June", "юни")
     .when(col("CalendarMonth") == "July", "юли")
     .when(col("CalendarMonth") == "August", "авг")
     .when(col("CalendarMonth") == "September", "сеп")
     .when(col("CalendarMonth") == "October", "окт")
     .when(col("CalendarMonth") == "November", "ное")
     .when(col("CalendarMonth") == "December", "дек")
     .otherwise(None))
 .withColumn("DevbgDate", concat_ws(" ", "DayOfMonth", "DevbgMonth"))
 .withColumn("DateZaplata", concat_ws(" ", lpad(df_date_filtered["DayOfMonth"], 2, "0"), "CalendarMonth"))
) 

# COMMAND ----------

# DBTITLE 1,Prepare DevBg
# # Align dates between DimDate and DevBg posts data
# df_posts_devbg = df_posts_devbg.withColumn("Uploaded", (expr("replace(Uploaded, '.', '')")))

# # Join DevBg posts data with DimDate to obtain date information
# df_posts_devbg = df_posts_devbg.alias("df_posts_devbg")
# df_date_filtered = df_date_filtered.alias("df_date_filtered")

# df_devbg_final = (
#     df_posts_devbg
#     .join(df_date_filtered, df_posts_devbg.Uploaded == df_date_filtered.DevbgDate, how = "left")
#     .select("df_posts_devbg.*", "df_date_filtered.CalendarDate")
# #     .withColumnRenamed("Date", "UploadedCalc")
# )

# df_devbg_final.display()

# COMMAND ----------

# DBTITLE 1,Prepare ZaplataBg
# df_posts_zaplata = df_posts_zaplata.alias("df_posts_zaplata")
# df_date_filtered = df_date_filtered.alias("df_date_filtered")

# df_zaplata_final = (
#     df_posts_zaplata
#     .join(df_date_filtered, df_posts_zaplata.DatePosted == df_date_filtered.DateZaplata, how = "left")
#     .select("df_posts_zaplata.*", "df_date_filtered.CalendarDate")
# #     .withColumnRenamed("Date", "DatePostedCalc")
# )

# df_zaplata_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union sources

# COMMAND ----------

# df_fct_posts = (
#     df_devbg_final
#     .select(concat("Link", "Department").alias("JobPostId"), col("CalendarDate").alias("DatePosted"), lit(1).alias("SourceSystemKey"))
#     .union(df_zaplata_final.select(col("JobId").alias("JobPostId"), col("CalendarDate").alias("DatePosted"), lit(2).alias("SourceSystemKey")))
#     .union(df_posts_noblehire.select(col("id").alias("JobPostId"), date_format(col("postedAt_Timestamp"), "yyyy-MM-dd").alias("DatePosted"), lit(3).alias("SourceSystemKey"))
#           )
# )
    
# df_fct_posts.display()

# COMMAND ----------

df_posts_noblehire.display()


# COMMAND ----------

# Add location

df_fct_posts = (
    df_posts_noblehire
    .select(
        col("id").alias("JobPostId"), 
        date_format(col("postedAt_Timestamp"), "yyyyMMdd").alias("DatePosted"), 
        lit(3).alias("SourceSystemKey"), 
        col("id").alias("ActivitiesId"),
        col("companyId").alias("AwardsId"),
        col("id").alias("BenefitsId"),
        col("companyId").alias("CompanyId"),
        col("id").alias("HiringProcessId"),
        col("id").alias("LocationId"),
        col("companyId").alias("PerksId"),
        col("id").alias("RequirementsId"),
        col("id").alias("ResponsibilitiesId"),
        col("id").alias("ToolsId"),
        col("companyId").alias("ValuesId"),
        col("id").alias("JunkId")
    )
)

df_fct_posts.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to ADLS

# COMMAND ----------

# DBTITLE 1,Create FctPosts
df_fct_posts.write.format("delta").mode("overwrite").option("path", "/mnt/adlslirkov/it-job-boards/Warehouse/FctPosts").saveAsTable("WAREHOUSE.FctPosts")
