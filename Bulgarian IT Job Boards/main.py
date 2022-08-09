# Databricks notebook source
import requests
from bs4 import BeautifulSoup

# COMMAND ----------

#===========================================================================================================
#----------------------------- Bulgarian IT Job Boards - Web Scraping Project ------------------------------
#===========================================================================================================

# Class to scrape "https://dev.bg/"
class scrape_devbg:
    
    def __init__(self, department):
        self.department = department

    def parseHtml(self, page=None, url=None):
        headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"}
        if url == None:
            url = f"https://dev.bg/company/jobs/{self.department}/?_paged={page}"
        r = requests.get(url, headers)
        soup = BeautifulSoup(r.content, "html.parser")
        return soup
    
    # Scrape job description by passing a job posts link. Links are obtained by the scrapeJobPost method.
    def scrapeJobDescription(self, soup, link, target_list):
        divs = soup.find_all("div", class_ =  "single_job_listing")
        for tag in divs:
            try:
                job_description = tag.find("div", class_ = "job_description").text.strip()
            except:
                job_description = "Unknown"
            job_description = {
                "link": link,
                "job_description": job_description
            }
            target_list.append(job_description)
        return 
                
    # Scrape job posts by looking at the main page. 
    # Link to the detailed job description page is also scraped and can be passed to the scrapeJobDescription method.
    def scrapeJobPost(self, soup, target_list):
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
                "department": self.department,
                "link": link
            }
            target_list.append(job)
        return 

    # Returns the results per page
    def getPageResults(self, page):
        soup = self.parseHtml(page)
        divs = soup.find_all("div", class_ =  "job-list-item")
        return len(divs)

    # Returns the total count of pages, based on results per page != 0
    def getPageCount(self):
        page = 1
        while self.getPageResults(page) != 0:
            page += 1
        return page - 1
      
        
# Class to scrape "https://noblehire.io/"        
class scrape_Noblehire():
    url = "https://prod-noblehire-api-000001.appspot.com/job?"
    
    # Returns all posts for a given page as Pandas DataFrame
    def getPosts(self, page):
        page = f"&page={page}"
        url = scrape_Noblehire.url + page
        response = requests.request("GET", url)
        return response.json()["elements"]
