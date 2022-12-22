# Databricks notebook source
# MAGIC %run "lirkov/IT Job Boards/Main"

# COMMAND ----------

# DBTITLE 1,Define variables
# Job posts departments
departments = [
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

# Attributes of companies
company_attributes = {
    "locations":["locationsofia", "locationplovdiv", "locationvarna", "locationburgas", "locationruse"],
    "headquarters":["v-balgaria", "v-chuzhbina"],
    "employees":["1-9", "10-30", "31-70", "70"],
    "company-activity":["produktovi-kompanii", "it-konsultirane", "survis-kompanii", "vnedrjavane-na-softuerni-sistemi"],
    "paid-leave":["20-dni", "21-25-dni", "25-dni"],
    "work-hours":["iztsyalo-guvkavo", "chastichno-guvkavo", "fiksirano"]
}

# Date variables 
current_year = date.today().year
current_month = "0" + str(date.today().month) if len(str(date.today().month)) == 1 else date.today().month
current_day = "0" + str(date.today().day) if len(str(date.today().day)) == 1 else date.today().day

# Raw location variables
location_prefix = "/dbfs"
main_path = "/mnt/adlslirkov/it-job-boards/DEV.bg/raw/"
posts_path = f"posts/{current_year}/{current_month}/{current_day}/"
descriptions_path = f"descriptions/{current_year}/{current_month}/{current_day}/"
company_location_path = f"companyLocation/{current_year}/{current_month}/{current_day}/"
company_headquarters_path = f"companyHeadquarters/{current_year}/{current_month}/{current_day}/"
company_employees_count_path = f"companyEmployeesCount/{current_year}/{current_month}/{current_day}/"
company_activity_path = f"companyActivity/{current_year}/{current_month}/{current_day}/"
company_paid_leave_path = f"companyPaidLeave/{current_year}/{current_month}/{current_day}/"
company_work_hours_path = f"companyWorkHours/{current_year}/{current_month}/{current_day}/"

# Print RAW locations
print(f"Posts path: {posts_path}")
print(f"Descriptions path: {descriptions_path}")
print(f"Company location path: {company_location_path}")
print(f"Company headquarters path: {company_headquarters_path}")
print(f"Company employees count path: {company_employees_count_path}")
print(f"Company activity path: {company_activity_path}")
print(f"Company paid leave path: {company_paid_leave_path}")
print(f"Company work hours path: {company_work_hours_path}")

# COMMAND ----------

# DBTITLE 1,Initiate scrape
# Create class instance
scraper = scrape_devbg()


# Create empty lists to hold scraped data for each category

# Job posts 
jobPosts = []

# Job descriptions
jobDescription = []

# Company location
companiesSofia = []
companiesPlovdiv = []
companiesVarna = []
companiesBurgas = []
companiesRuse = []

# Company headquarters
headquartersVbulgaria = []
headquartersVchuzhbina = []

# Company size (in terms of number of employees)
employees1_9 = []
employees10_30 = []
employees31_70 = []
employees70 = []

# Company activity
activityProduktoviKompanii = []
activityItKonsultirane = []
activitySurvisKompanii = []
activityVnedrjavaneNaSoftuerniSistemi = []

# Company paid leave offered
paidLeave20 = []
paidLeave21_25 = []
paidLeave25 = []

# Company working hours
workHoursIztsyaloGuvkavo = []
workHoursChastichnoGuvkavo = []
workHoursFiksirano = []

# COMMAND ----------

# DBTITLE 1,Scrape job posts
# Execute scrapeJobPosts for all departments and all pages
for department in departments:
    for page in range(1, scraper.getPageCount(department) + 1):
        scraper.scrapeJobPosts(department, page, jobPosts)
        print(f"Scraping page {page} of department {department}...")

# Create job posts DataFrame    
df_jobposts = spark.createDataFrame(jobPosts)

# COMMAND ----------

# DBTITLE 1,Scrape job descriptions
# Execute scrapeJobDescription for all job post links scraped by the above command
# for job in jobPosts:
#     scraper.scrapeJobDescriptions(url=job["link"], target_list = jobDescription)
#     print(f"Scraping job descriptions in {department} department...")
    
# # Create job descriptions DataFrame    
# df_jobdescriptions = spark.createDataFrame(jobDescription)    

# COMMAND ----------

# DBTITLE 1,Scrape companies by location
# Execute scrapeCompany for all company locations
scraper.scrapeCompany(category="select", attribute="locationsofia", target_list=companiesSofia)
scraper.scrapeCompany(category="select", attribute="locationplovdiv", target_list=companiesPlovdiv)
scraper.scrapeCompany(category="select", attribute="locationvarna", target_list=companiesVarna)
scraper.scrapeCompany(category="select", attribute="locationburgas", target_list=companiesBurgas)
scraper.scrapeCompany(category="select", attribute="locationruse", target_list=companiesRuse)

# Append all company location lists together
companyLocations = []
[companyLocations.extend(l) for l in (companiesSofia, companiesPlovdiv, companiesVarna, companiesBurgas, companiesRuse)]

# Create company locations DataFrame  
df_companylocations = spark.createDataFrame(data=companyLocations, schema=["Company", "Subscription", "Location"])

# COMMAND ----------

# DBTITLE 1,Scrape companies by headquarters
# Execute scrapeCompany for all company headquarters
scraper.scrapeCompany(category="headquarters", attribute="v-balgaria", target_list=headquartersVbulgaria)
scraper.scrapeCompany(category="headquarters", attribute="v-chuzhbina", target_list=headquartersVchuzhbina)

# Add headquarter to headquartersVbulgaria list
headquartersVbulgaria = [[h, "v-balgaria"] for h in headquartersVbulgaria]

# Add headquarter to headquartersVchuzhbina list
headquartersVchuzhbina = [[h, "v-chuzhbina"] for h in headquartersVchuzhbina]

# Append headquarters lists 
companyHeadquarters = []
[companyHeadquarters.extend(h) for h in (headquartersVbulgaria, headquartersVchuzhbina)]

# Create company headquarters DataFrame
df_companyheadquarters = spark.createDataFrame(data=companyHeadquarters, schema=["Company", "Headquarter"])

# COMMAND ----------

# DBTITLE 1,Scrape companies by count of employees
# Execute scrapeCompany for all company employees count
scraper.scrapeCompany(category="it-employees", attribute="1-9", target_list=employees1_9)
scraper.scrapeCompany(category="it-employees", attribute="10-30", target_list=employees10_30)
scraper.scrapeCompany(category="it-employees", attribute="31-70", target_list=employees31_70)
scraper.scrapeCompany(category="it-employees", attribute="70", target_list=employees70)

# Add employees count to employees1_9 list
employees1_9 = [[ec, "1-9"] for ec in employees1_9]

# Add employees count to employees10_30 list
employees10_30 = [[ec, "10-30"] for ec in employees10_30]

# Add employees count to employees31_70 list
employees31_70 = [[ec, "31-70"] for ec in employees31_70]

# Add employees count to employees70 list
employees70 = [[ec, "70"] for ec in employees70]

# Append employees count lists 
employeesCount = []
[employeesCount.extend(ec) for ec in (employees1_9, employees10_30, employees31_70, employees70)]

# Create company employees count DataFrame
df_employeescount = spark.createDataFrame(data=employeesCount, schema=["Company", "EmployeesCount"])

# COMMAND ----------

# DBTITLE 1,Scrape companies by activity
# Execute scrapeCompany for all company activities
scraper.scrapeCompany(category="company-activity", attribute="produktovi-kompanii", target_list=activityProduktoviKompanii)
scraper.scrapeCompany(category="company-activity", attribute="it-konsultirane", target_list=activityItKonsultirane)
scraper.scrapeCompany(category="company-activity", attribute="survis-kompanii", target_list=activitySurvisKompanii)
scraper.scrapeCompany(category="company-activity", attribute="vnedrjavane-na-softuerni-sistemi", target_list=activityVnedrjavaneNaSoftuerniSistemi)

# Add company activity to activityProduktoviKompanii list
activityProduktoviKompanii = [[a, "1-9"] for a in activityProduktoviKompanii]

# Add company activity to activityItKonsultirane list
activityItKonsultirane = [[a, "10-30"] for a in activityItKonsultirane]

# Add company activity to activitySurvisKompanii list
activitySurvisKompanii = [[a, "31-70"] for a in activitySurvisKompanii]

# Add company activity to activityVnedrjavaneNaSoftuerniSistemi list
activityVnedrjavaneNaSoftuerniSistemi = [[a, "70"] for a in activityVnedrjavaneNaSoftuerniSistemi]

# Append company activities lists 
companyActivities = []
[companyActivities.extend(a) for a in (activityProduktoviKompanii, activityItKonsultirane, activitySurvisKompanii, activityVnedrjavaneNaSoftuerniSistemi)]

# Create company activities DataFrame
df_companyactivities = spark.createDataFrame(data=companyActivities, schema=["Company", "Activity"])

# COMMAND ----------

# DBTITLE 1,Scrape companies by paid leaves
# Execute scrapeCompany for all company paid leaves
scraper.scrapeCompany(category="paid-leave", attribute="20-dni", target_list=paidLeave20)
scraper.scrapeCompany(category="paid-leave", attribute="21-25-dni", target_list=paidLeave21_25)
scraper.scrapeCompany(category="paid-leave", attribute="25-dni", target_list=paidLeave25)

# Add company paid leaves to paidLeave20 list
paidLeave20 = [[pl, "20-dni"] for pl in paidLeave20]

# Add company paid leaves to leave_21_25 list
paidLeave21_25 = [[pl, "21-25-dni"] for pl in paidLeave21_25]

# Add company paid leaves to paidLeave25 list
paidLeave25 = [[pl, "25-dni"] for pl in paidLeave25]

# Append company paid leaves lists 
companyPaidLeaves = []
[companyPaidLeaves.extend(pl) for pl in (paidLeave20, paidLeave21_25, paidLeave25)]

# Create company paid leaves DataFrame
df_companypaidleaves = spark.createDataFrame(data=companyPaidLeaves, schema=["Company", "PaidLeave"])

# COMMAND ----------

# DBTITLE 1,Scrape companies by work hours
# Execute scrapeCompany for all company work hours
scraper.scrapeCompany(category="work-hours", attribute="iztsyalo-guvkavo", target_list=workHoursIztsyaloGuvkavo)
scraper.scrapeCompany(category="work-hours", attribute="chastichno-guvkavo", target_list=workHoursChastichnoGuvkavo)
scraper.scrapeCompany(category="work-hours", attribute="fiksirano", target_list=workHoursFiksirano)

# Add company work hours to workHoursIztsyaloGuvkavo list
workHoursIztsyaloGuvkavo = [[wa, "iztsyalo-guvkavo"] for wa in workHoursIztsyaloGuvkavo]

# Add company work hours to workHoursChastichnoGuvkavo list
workHoursChastichnoGuvkavo = [[wa, "chastichno-guvkavo"] for wa in workHoursChastichnoGuvkavo]

# Add company work hours to workHoursFiksirano list
workHoursFiksirano = [[wa, "fiksirano"] for wa in workHoursFiksirano]

# Append company work hours lists 
companyWorkHours = []
[companyWorkHours.extend(wa) for wa in (workHoursIztsyaloGuvkavo, workHoursChastichnoGuvkavo, workHoursFiksirano)]

# Create company work hours DataFrame
df_companyworkhours = spark.createDataFrame(data=companyWorkHours, schema=["Company", "WorkHours"])

# COMMAND ----------

# DBTITLE 1,Write to ADLS (Raw)
# Write the job posts DataFrame to ADLS, raw location
df_jobposts.write.format("parquet").save(main_path + posts_path)
print(f"Job posts saved at: {main_path + posts_path}")

# Write the job description DataFrame to ADLS, raw location
# df_jobdescriptions.write.format("parquet").save(main_path + descriptions_path + descriptions_file_name)
# print(f"Job descriptions saved at: {main_path + descriptions_path + descriptions_file_name}")

# Write the company locations DataFrame to ADLS, raw location
df_companylocations.write.mode("overwrite").format("parquet").save(main_path + company_location_path)
print(f"Company locations saved at: {main_path + company_location_path}")

# Write the company headquarters DataFrame to ADLS, raw location
df_companyheadquarters.write.mode("overwrite").format("parquet").save(main_path + company_headquarters_path)
print(f"Company headquarters saved at: {main_path + company_headquarters_path}")

# Write the company employees count DataFrame to ADLS, raw location
df_employeescount.write.mode("overwrite").format("parquet").save(main_path + company_employees_count_path)
print(f"Company employees count saved at: {main_path + company_employees_count_path}")

# Write the company activities DataFrame to ADLS, raw location
df_companyactivities.write.mode("overwrite").format("parquet").save(main_path + company_activity_path)
print(f"Company activities saved at: {main_path + company_activity_path}")

# Write the company paid leave DataFrame to ADLS, raw location
df_companypaidleaves.write.mode("overwrite").format("parquet").save(main_path + company_paid_leave_path)
print(f"Company paid leave saved at: {main_path + company_paid_leave_path}")

# Write the company work hours DataFrame to ADLS, raw location
df_companyworkhours.write.mode("overwrite").format("parquet").save(main_path + company_work_hours_path)
print(f"Company work hours saved at: {main_path + company_work_hours_path}")
