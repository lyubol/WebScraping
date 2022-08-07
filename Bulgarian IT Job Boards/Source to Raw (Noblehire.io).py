# Databricks notebook source
from datetime import date
import time
import pandas as pd

# COMMAND ----------

# MAGIC %run "lirkov/IT Job Boards/Main"

# COMMAND ----------

# DBTITLE 1,Define variables
# Raw location variables
location_prefix = "/dbfs"
main_path = "/mnt/adlslirkov/it-job-boards/Noblehire.io/raw/"
posts_path = f"posts/{date.today().year}/{date.today().month}/{date.today().day}/"
posts_file_name = f"noblehireio-posts-{date.today()}.csv"

# COMMAND ----------

# DBTITLE 1,Scrape job posts
posts = scrape_Noblehire()

page = 0
df_posts = posts.getPosts(page)
while posts.getPosts(page).empty == False:
    page += 1
    df_posts = pd.concat([df_posts, posts.getPosts(page)])
    time.sleep(10)

# COMMAND ----------

# DBTITLE 1,Write to ADLS Raw
# Create target location
dbutils.fs.mkdirs(main_path + posts_path)

# Write the Posts DataFrame to ADLS, raw location
df_posts.to_csv(location_prefix + main_path + posts_path + posts_file_name)

