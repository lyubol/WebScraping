def extract(page):
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"}
    url = f"https://dev.bg/company/jobs/data-science/?_paged={page}"
    r = requests.get(url, headers)
    soup = BeautifulSoup(r.content, "html.parser")
    return soup


def transform(soup):
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
            "salary": salary
        }
        jobs.append(job)
    return 


def get_page_results(page: int) -> int:
    soup = extract(page)
    divs = soup.find_all("div", class_ =  "job-list-item")
    return len(divs)
    
    
def get_page_count():
    page = 1
    while get_page_results(page) != 0:
        page += 1
    return page - 1



# Execute

# jobs = []
# for page in range(1, get_page_count() + 1):
#     transform(extract(page))