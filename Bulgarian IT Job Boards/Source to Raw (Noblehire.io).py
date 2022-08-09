# Databricks notebook source
from datetime import date
import time
import pandas as pd
from flatten_json import flatten

# COMMAND ----------

# MAGIC %run "lirkov/IT Job Boards/Main"

# COMMAND ----------

# DBTITLE 1,Define variables
# Raw location variables
location_prefix = "/dbfs"
main_path = "/mnt/adlslirkov/it-job-boards/Noblehire.io/raw/"
posts_path = f"posts/{date.today().year}/{date.today().month}/{date.today().day}/"
posts_file_name = f"noblehireio-posts-{date.today()}.csv"

print(f"Posts path: {posts_path}; Posts file name: {posts_file_name}")

# COMMAND ----------

# DBTITLE 1,Scrape job posts
posts = scrape_Noblehire()

page = 0
flatten_posts_list = []
while len(posts.getPosts(page)) != 0:
    page += 1
    posts_response = posts.getPosts(page)
    for post in posts_response:
        flatten_posts_list.append(flatten(post))
    time.sleep(10)

# COMMAND ----------

# DBTITLE 1,Write to ADLS Raw
# Create target location
dbutils.fs.mkdirs(main_path + posts_path)
print(f"Created: {main_path + posts_path}")

# Since the raw data has more than 700 columns and above 300 of them are useless (e.g. icons and images ids), 
# some of the will be droped in order to decrease the raw file size.
df_posts = pd.DataFrame.from_dict(flatten_posts_list)
columns_to_drop = [c for c in df.columns if "icon" in c or "images" in c]
df = df.drop(columns=columns_to_drop)

# Write the Posts DataFrame to ADLS, raw location
df_posts.to_csv(location_prefix + main_path + posts_path + posts_file_name)
print(f"Saved at: {location_prefix + main_path + posts_path + posts_file_name}")

# COMMAND ----------

# df_posts["description"] = df_posts["description"].str.replace(r'<[^<>]*>', '', regex=True)

# COMMAND ----------

# for i in df_posts["description"]:
#     print(i)
#     print("------------------------------------------NEXT------------------------------------------")

# COMMAND ----------

# for i in df_posts["company"]:
#     print(i)
#     print("------------------------------------------NEXT------------------------------------------")
