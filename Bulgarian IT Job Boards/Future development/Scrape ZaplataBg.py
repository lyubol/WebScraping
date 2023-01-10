# Databricks notebook source
# MAGIC %md
# MAGIC ### Initial version

# COMMAND ----------

import requests
import pandas as pd
from bs4 import BeautifulSoup

# Create a list to hold job posts as dictionaries
jobPosts = []

for i in range(12):

    URL = f"https://www.zaplata.bg/en/search/?q=&city=&city_distance=0&go=&cat%5B0%5D=12&cat%5B1%5D=13&price=200%3B10000&page={i}"
    r = requests.get(URL)

    soup = BeautifulSoup(r.content, "html.parser")

    # Get all list items
    div = soup.find_all("div", class_ =  "listItems")
    # For each list item
    for tag in div:
        ul = tag.find_all("ul", class_ = "listItem")
        # If the job offer type is 'PRO' or 'TOP' or VIP', it won't have a salary.
        # Other HTML tags are slightly different as well.
        # First, we get the job offer type for each list item.
        for li in ul:
            try:
                postType = li.find("li", class_ = "c1").img["title"]
            except TypeError:
                postType = None

            # If the current job offer type is among the special offer types, execute the following:    
            if postType in ["PRO job offers — Zaplata.bg", "TOP job offers — Zaplata.bg", "VIP job offers — Zaplata.bg"]:
                company = li.find("li", class_ = "c4").text.strip()
                jobTitle = li.find("li", class_ = "c2").a.text.strip()
                jobInfo = li.find("li", class_ = "c2").text.strip()

                # Create a dictionary for each job post
                jobDict = {
                    "company": company,
                    "job title": jobTitle,
                    "min salary": None,
                    "max salary": None,
                    "date posted": jobInfo.split("\n")[1].split(",")[0],
                    "location": jobInfo.split("\n")[1].split(",")[1].strip(),
                    "job offer type": postType
                }

                # Append the above created dictionary to a list
                jobPosts.append(jobDict)

            # If the current job offer type is not among the special offer types, execute the following:
            else:
                company = li.find("li", class_ = "c4").text.strip()
                jobTitle = li.find("li", class_ = "c2").a.text.strip()
                jobInfo = li.find("li", class_ = "c2").text.strip()

                # Create a dictionary for each job post
                jobDict = {
                    "company": company,
                    "job title": jobTitle,
                    "min salary": jobInfo.split("\n")[1].split("to")[0].replace("Salary from:", "").strip(),
                    "max salary": jobInfo.split("\n")[1].split("to")[1].replace("BGN", "").strip(),
                    "date posted": jobInfo.split("\n")[2].split(",")[0],
                    "location": jobInfo.split("\n")[2].split(",")[1].strip(),
                    "job offer type": postType
                }

                # Append the above created dictionary to a list
                jobPosts.append(jobDict)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upgrade initial version and implement OOP

# COMMAND ----------

class scrape_zaplatabg:
    
    def __init__(self):
        self.headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"}
        self.jobPostsURL = "https://www.zaplata.bg/en/search/?q=&city=&city_distance=0&go=&cat%5B0%5D=12&cat%5B1%5D=13&price=200%3B10000"

    def getJobPostsCount(self, page):
        r = requests.get(self.jobPostsURL + "&page=" + str(page))
        soup = BeautifulSoup(r.content, "html.parser")
        # Get all list items
        div = soup.find_all("div", class_ =  "listItems")
        # For each list item
        for tag in div:
            ul = tag.find_all("ul", class_ = "listItem")
            responseLength = len(ul)
        return responseLength
    
    def getTotalPageCount(self):
        page = 1
        while getJobPostsCount(page) != 0:
            page += 1
        return page - 1

# COMMAND ----------

test = scrape_zaplatabg()
test.getTotalPageCount()
