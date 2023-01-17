# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# MAGIC %run "lirkov/IT Job Boards/Main"

# COMMAND ----------

# DBTITLE 1,Define variables
# Date variables 
current_year = date.today().year
current_month = "0" + str(date.today().month) if len(str(date.today().month)) == 1 else date.today().month
current_day = "0" + str(date.today().day) if len(str(date.today().day)) == 1 else date.today().day

# Raw location variables
location_prefix = "/dbfs"
main_path = "/mnt/adlslirkov/it-job-boards/Zaplata.bg/raw/"
posts_path = f"posts/{current_year}/{current_month}/{current_day}/"

print(f"Posts path: {main_path}{posts_path}")

# COMMAND ----------

# DBTITLE 1,Scrape job posts
# Instantiate class 
scraper = scrape_zaplatabg()

# Create empty list to append job posts
jobPosts = []

# Get the count of all pages
totalPages = scraper.getTotalPageCount()

# Scrape job posts for each page
for page in range(1, totalPages + 1):
    scraper.scrapeJobPosts(page, jobPosts)
    
print(f"{len(jobPosts)} posts scraped")

# COMMAND ----------

# DBTITLE 1,Write to ADLS (Raw)
# Define schema
jobposts_schema = StructType(fields = [
    StructField("JobLink", StringType(), True),
    StructField("Company", StringType(), True),
    StructField("JobTitle", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("MaxSalary", StringType(), True),
    StructField("MinSalary", StringType(), True),
    StructField("DatePosted", StringType(), True),
    StructField("JobOfferType", StringType(), True)
])

# Create DataFrame
df_jobposts = spark.createDataFrame(data=jobPosts, schema = jobposts_schema)

# Write DataFrame to ADLS (Raw)
df_jobposts.write.format("parquet").save(f"{main_path}{posts_path}")
