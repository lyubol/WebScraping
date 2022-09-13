# Databricks notebook source
from datetime import date
import time
import pandas as pd

# COMMAND ----------

# MAGIC %run "lirkov/IT Job Boards/Main"

# COMMAND ----------

# DBTITLE 1,Define variables
devbg_departments = [
    "back-end-development", 
    "mobile-development",
    "full-stack-development",
    "front-end-development",
    "pm-ba-and-more",
    "operations",
    "quality-assurance",
    "erp-crm-development",
    "ui-ux-and-arts",
    "data-science",
    "technical-support"
]

# Date variables 
current_year = date.today().year
current_month = "0" + str(date.today().month) if len(str(date.today().month)) == 1 else date.today().month
current_day = "0" + str(date.today().day) if len(str(date.today().day)) == 1 else date.today().day

# Raw location variables
location_prefix = "/dbfs"
main_path = "/mnt/adlslirkov/it-job-boards/DEV.bg/raw/"
posts_path = f"posts/{current_year}/{current_month}/{current_day}/"
posts_file_name = f"devbg-posts-{date.today()}.csv"
descriptions_path = f"descriptions/{current_year}/{current_month}/{current_day}/"
descriptions_file_name = f"devbg-descriptions-{current_day}.csv"

print(f"Posts path: {posts_path}; Posts file name: {posts_file_name}")

# COMMAND ----------

# DBTITLE 1,Scrape job posts
# Create list to hold posts data. Each post will be appended as a dictionary
job_posts = []

# Execute scrapeJobPosts for all departments and all pages
for department in devbg_departments:
    posts = scrape_devbg(department)
    time.sleep(10)
    for page in range(1, posts.getPageCount() + 1):
        time.sleep(1)
        posts.scrapeJobPost(posts.parseHtml(page=page), job_posts)
        
# Create Posts DataFrame        
df_posts = pd.DataFrame.from_dict(job_posts)

# COMMAND ----------

# DBTITLE 1,Write to ADLS (Raw)
# Create target location
dbutils.fs.mkdirs(main_path + posts_path)
print(f"Created: {main_path + posts_path}")

# Write the Posts DataFrame to ADLS, raw location
df_posts.to_csv(location_prefix + main_path + posts_path + posts_file_name)
print(f"Saved at: {location_prefix + main_path + posts_path + posts_file_name}")

# COMMAND ----------

# DBTITLE 1,Scrape job descriptions
# # Create list to hold descriptions data. Each description will be appended as a dictionary
# job_description = []
# links = df_posts["link"].to_list()

# # Execute scrapeJobDescription each link in the posts DataFrame
# for link in links:
#     test.scrapeJobDescription(test.parseHtml(url=job_link), link, job_description)
    
# # Create Descriptions DataFrame       
# df_descriptions = pd.DataFrame.from_dict(job_description)

# COMMAND ----------

# DBTITLE 1,Write to ADLS
# # Create target location
# dbutils.fs.mkdirs(main_path + descriptions_path)

# # Write the Descriptions DataFrame to ADLS, raw location
# df_descriptions.to_csv(location_prefix + main_path + descriptions_path + descriptions_file_name)

# COMMAND ----------

# %run "lirkov/IT Job Boards/Raw to Base to Delta"
