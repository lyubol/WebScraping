import requests
import pandas as pd
from bs4 import BeautifulSoup


class scrape_devbg:
    
    def __init__(self, department):
        self.department = department

    def parse_html(self, page):
        headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"}
        url = f"https://dev.bg/company/jobs/{self.department}/?_paged={page}"
        r = requests.get(url, headers)
        soup = BeautifulSoup(r.content, "html.parser")
        return soup


    def obtain_data(self, soup, target_list):
        divs = soup.find_all("div", class_ =  "job-list-item")
        for tag in divs:
            title = tag.find("h6").text.strip()
            company = tag.find("span").text.strip()
            location = tag.find("span", class_ = "badge").text.strip()
            uploaded = tag.find("span", class_ = "date date-with-icon").text.strip()
            try:
                salary = tag.find("span", class_ = "badge blue has-hidden-text has-tooltip").text.strip()
                salary = salary.split("лв.")[0]
            except:
                salary = "Unknown"

            job = {
                "title": title,
                "company": company,
                "location": location,
                "uploaded": uploaded,
                "salary": salary,
                "department": self.department
            }
            target_list.append(job)
        return 


    def get_page_results(self, page):
        soup = self.parse_html(page)
        divs = soup.find_all("div", class_ =  "job-list-item")
        return len(divs)


    def get_page_count(self):
        page = 1
        while self.get_page_results(page) != 0:
            page += 1
        return page - 1
