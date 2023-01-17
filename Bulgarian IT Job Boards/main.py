# Databricks notebook source
from datetime import date
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pyspark.sql.functions import *

# COMMAND ----------

#===========================================================================================================
#----------------------------- Bulgarian IT Job Boards - Web Scraping Project ------------------------------
#===========================================================================================================

# Class to scrape "https://dev.bg/"
class scrape_devbg:
    

    def __init__(self):
        self.headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"}
        self.jobPostsURL = "https://dev.bg/company/jobs/"
        self.companyURL = "https://dev.bg/company/"
    

    # Scrape job posts by looking at the main page. 
    # Link to the detailed job description page is also scraped and can be passed to the scrapeJobDescription method to scrape each job's description.
    def scrapeJobPosts(self, department, page, target_list):
        fullJobPostsURL = self.jobPostsURL + department + "/?_paged=" + str(page)
        response = requests.get(fullJobPostsURL, self.headers)
        soup = BeautifulSoup(response.content, "html.parser")
        divs = soup.find_all("div", class_ = "job-list-item")
        for tag in divs:
            try:
                title = tag.find("h6").text.strip()
            except:
                title = "Unknown"
            try:
                company = tag.find("span").text.strip()
            except:
                company = "Unknown"
            try:
                location = tag.find("span", class_ = "badge").text.strip()
            except:
                location = "Unknown"
            uploaded = tag.find("span", class_ = "date date-with-icon").text.strip()
            try:
                salary = tag.find("span", class_ = "badge blue has-hidden-text has-tooltip").text.strip()
                salary = salary.split("лв.")[0]
            except:
                salary = "Unknown"
            try:
                link = tag.find("a", class_ = "overlay-link ab-trigger")["href"]
            except:
                link = "Unknown"
            job = {
                "title": title,
                "company": company,
                "location": location,
                "uploaded": uploaded,
                "salary": salary,
                "department": department,
                "link": link
            }
            target_list.append(job)
        return 
    

    # Scrape job description by passing a job posts link. Links are obtained by the scrapeJobPost method.
    def scrapeJobDescriptions(self, url, target_list):
        response = requests.get(url, self.headers)
        soup = BeautifulSoup(response.content, "html.parser")
        divs = soup.find_all("div", class_ =  "single_job_listing")
        for tag in divs:
            try:
                job_description = tag.find("div", class_ = "job_description").text.strip()
            except:
                job_description = "Unknown"
            job_description = {
                "link": url,
                "job_description": job_description
            }
            target_list.append(job_description)
        return 
    

    # Scrape company data based on different categories and attributes. Each category and its attributes are provided below.
    # select(locations) - locationsofia, locationplovdiv, locationvarna, locationburgas, locationruse;
    # headquarters - v-balgaria, v-chuzhbina;
    # employees - 1-9, 10-30, 31-70, 70
    # company_activity - produktovi-kompanii, it-konsultirane, survis-kompanii, vnedrjavane-na-softuerni-sistemi
    # paid_leave - 20-dni, 21-25-dni, 25-dni
    # work_hours - iztsyalo-guvkavo, chastichno-guvkavo, fiksirano
    def scrapeCompany(self, category, attribute, target_list):
        fullCompanyURL = self.companyURL + category + "/" + attribute + "/"
        response = requests.get(fullCompanyURL, self.headers)
        soup = BeautifulSoup(response.content, "html.parser")
        # if extracting company data by location, include if it is a premium or non-premium company
        if category == "select":
            # list to hold all premium companies
            premium_companies = []
            a_premium = soup.find_all("a", class_ = "mini-company-item premium-company")
            for tag in a_premium:
                premium_companies.append(tag["title"])
            a_other = soup.find_all("a", class_ = "mini-company-item")
            # go over all companies and check if a company is in the list of premium companies or not
            for tag in a_other:
                if tag["title"] in premium_companies:
                    temp_list = [tag["title"], "premium", attribute]
                else:
                    temp_list = [tag["title"], "non-premium", attribute]
                # append each company dictionary to the target list
                target_list.append(temp_list)
        # if extracting other company data
        else:
            a = soup.find_all("a", class_ = "mini-company-item")
            for tag in a:
                target_list.append(tag["title"])
        return
        

    # Returns the results per page for job posts
    def getPageResults(self, department, page):
        # Pass job department and page number; 
        # Get the number of results on the given page.
        fullJobPostsURL = self.jobPostsURL + department + '/?_paged=' + str(page)
        response = requests.get(fullJobPostsURL, self.headers)
        soup = BeautifulSoup(response.content, "html.parser")
        divs = soup.find_all("div", class_ =  "job-list-item")
        return len(divs)


    # Returns the total count of pages, based on results per page != 0
    def getPageCount(self, department):
        page = 1
        while self.getPageResults(department, page) != 0:
            page += 1
        return page - 1
      
        
# Class to scrape "https://noblehire.io/"        
class scrape_Noblehire():
    
    def __init__(self):
        self.jobPostsURL = "https://prod-noblehire-api-000001.appspot.com/job?"
    
    # Returns all posts for a given page
    def getPosts(self, page):
        page = f"&page={page}"
        url = self.jobPostsURL + page
        response = requests.request("GET", url)
        return response.json()["elements"]
      
        
# Class to scrape "https://www.zaplata.bg/" 
# Since this site has many job categories, the following class is scraping only two of them:
#     IT - Development / Hardware Support
#     IT - Development / Software Support
class scrape_zaplatabg:
    
    
    def __init__(self):
        self.headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"}
        self.jobPostsURL = "https://www.zaplata.bg/en/search/?q=&city=&city_distance=0&go=&cat%5B0%5D=12&cat%5B1%5D=13&price=200%3B10000"

        
    # Returns the count of posts on a given page
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
    
    
    # Returns the total number of pages
    def getTotalPageCount(self):
        page = 1
        while self.getJobPostsCount(page) != 0:
            page += 1
        return page - 1
    
    # Returns all job posts on a given page
    def scrapeJobPosts(self, page, targetList):
        
        URL = f"{self.jobPostsURL}&page={page}"
        r = requests.get(URL)
    
        soup = BeautifulSoup(r.content, "html.parser")

        # Get all list items
        div = soup.find_all("div", class_ =  "listItems")
        
        # For each list item
        for tag in div:
            ul = tag.find_all("ul", class_ = "listItem")
            
            # For each element within a list item
            for li in ul:
                # Set post type
                try:
                    postType = li.find("li", class_ = "c1").img["title"]
                except:
                    postType = None
                
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
                    
                # Set job link
                try:
                    jobLink = li.find("li", class_ = "c2").a["href"]
                except:
                    jobLink = None  
                
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
                    "JobLink": jobLink,
                    "Company": company,
                    "JobTitle": jobTitle,
                    "MinSalary": minSalary,
                    "MaxSalary": maxSalary,
                    "DatePosted": datePosted,
                    "Location": location,
                    "JobOfferType": postType
                }
    
                # Append the above created dictionary to a list
                targetList.append(jobDict)   
            
        return
