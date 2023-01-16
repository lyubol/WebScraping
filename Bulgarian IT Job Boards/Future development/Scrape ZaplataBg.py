# Databricks notebook source
# MAGIC %md
# MAGIC ### Initial version

# COMMAND ----------

import requests
import pandas as pd
from bs4 import BeautifulSoup

# Create a list to hold job posts as dictionaries
jobPosts = []

for i in range(1,14):
    URL = f"https://www.zaplata.bg/en/search/?q=&city=&city_distance=0&go=&cat%5B0%5D=12&cat%5B1%5D=13&price=200%3B10000&page={i}"
    r = requests.get(URL)

    soup = BeautifulSoup(r.content, "html.parser")

    # Get all list items
    div = soup.find_all("div", class_ =  "listItems")
    # For each list item
    for tag in div:
        ul = tag.find_all("ul", class_ = "listItem")

        for li in ul:
            # Set post type
            try:
                postType = li.find("li", class_ = "c1").img["title"]
            except:
                None
            
            # Set company name
            try:
                company = li.find("li", class_ = "c4").text.strip()
            except:
                company = None
            
            # Set job title
            try:
                jobTitle = li.find("li", class_ = "c2").a.text.strip()
            except:
                jobTitle = None  
            
            # Set additional job information
            jobInfo = li.find("li", class_ = "c2").text.strip()
            
            # Set salary min and max values
            try:
                salaryRange = jobInfo.split("\n")[-2]
            except:
                salaryRange = None
            
            try:
                minSalary = salaryRange.replace("Salary from:", "").split("to")[0].strip()
            except:
                minSalary = None
            try:
                assert str(minSalary).isnumeric()
            except AssertionError:
                minSalary = None
                
            try:
                maxSalary = salaryRange.replace("BGN", "").split("to")[-1].strip()
            except:
                maxSalary = None
            try:
                assert str(maxSalary).isnumeric()
            except AssertionError:
                maxSalary = None

            # Set location    
            try:
                location = jobInfo.split("\n")[-1].split(",")[1].strip()
            except:
                location = None
            
            # Set post date
            try:
                datePosted = jobInfo.split("\n")[-1].split(",")[0].strip()
            except:
                datePosted = None

            # Create a dictionary for each job post
            jobDict = {
                "company": company,
                "job title": jobTitle,
                "min salary": minSalary,
                "max salary": maxSalary,
                "date posted": datePosted,
                "location": location,
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
