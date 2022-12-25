# Databricks notebook source
from datetime import date
import time
import pandas as pd
import json
from flatten_json import flatten
from pyspark.sql.functions import *

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
main_path = "/mnt/adlslirkov/it-job-boards/Noblehire.io/raw/"
posts_path = f"posts/{current_year}/{current_month}/{current_day}/"

# print(f"Posts path: {main_path}{posts_path}; Posts file name: {posts_file_name}")

# COMMAND ----------

# DBTITLE 1,Create target location
dbutils.fs.mkdirs(main_path + posts_path)
print(f"Created: {main_path + posts_path}")

# COMMAND ----------

# DBTITLE 1,Scrape job posts
posts = scrape_Noblehire()

page = 0
while len(posts.getPosts(page)) != 0:
    page += 1
    posts_response = posts.getPosts(page)

    posts_response = json.dumps(posts_response)
    
    if len(posts_response) > 0:
        with open(f"{location_prefix}{main_path}{posts_path}{page}.json", "w") as f:
            f.write(posts_response)
            f.close()
    
    time.sleep(10)

# COMMAND ----------


