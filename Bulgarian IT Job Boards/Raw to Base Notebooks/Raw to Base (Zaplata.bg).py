# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# MAGIC %run "lirkov/IT Job Boards/Main"

# COMMAND ----------

# DBTITLE 1,Define variables
''# Date variables 
current_year = date.today().year
current_month = "0" + str(date.today().month) if len(str(date.today().month)) == 1 else date.today().month
current_day = "0" + str(date.today().day) if len(str(date.today().day)) == 1 else date.today().day

# Raw and Base location variables
location_prefix = "/dbfs"
main_path_raw = "/mnt/adlslirkov/it-job-boards/Zaplata.bg/raw/"
main_path_base = "/mnt/adlslirkov/it-job-boards/Zaplata.bg/base/"
posts_path = f"posts/{current_year}/{current_month}/{current_day}/"

# Print RAW locations
print(f"Posts RAW path: {main_path_raw}{posts_path}")

# Print BASE locations
print(f"Posts BASE path: {main_path_base}{posts_path}")

# COMMAND ----------

# DBTITLE 1,Read Raw data
df_jobposts = spark.read.format("parquet").load(f"{main_path_raw}{posts_path}")

# df_jobposts.display()

# COMMAND ----------

# DBTITLE 1,Adjust data types
df_jobposts = (df_jobposts
     .withColumn("JobLink", col("JobLink").cast(StringType()))          
     .withColumn("Company", col("Company").cast(StringType()))
     .withColumn("JobTitle", col("JobTitle").cast(StringType()))
     .withColumn("Location", col("Location").cast(StringType()))
     .withColumn("MaxSalary", col("MaxSalary").cast(IntegerType()))
     .withColumn("MinSalary", col("MinSalary").cast(IntegerType()))
     .withColumn("DatePosted", col("DatePosted").cast(StringType()))
     .withColumn("JobOfferType", col("JobOfferType").cast(StringType()))
)

# df_jobposts.display()

# COMMAND ----------

# DBTITLE 1,Add date and source columns
df_jobposts = (df_jobposts
  .withColumn("Source", lit("Zaplata.bg"))
  .withColumn("IngestionDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
)

df_jobposts.display()

# COMMAND ----------

# DBTITLE 1,Write to ADLS (Base)
df_jobposts.write.format("parquet").save(f"{main_path_base}{posts_path}")
