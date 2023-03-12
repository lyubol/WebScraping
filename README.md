# WebScraping

Repository owner: Lyubomir Lirkov
Repository creation date: 2021/09/03
Description: This repository is intended to store all my web scraping projects. 


The repository contains the following web scraping based projects:


* (In Progress...) Scraping IT Job Posts from the most popular Bulgarian IT job boards (DEV.bg, Noblehire.io Jobs.bg(TBC)):
  This is a data engineering project based on web scraping. 
  - Data is being scraped every day and stored in the RAW layer (source format) in an Azure Data Lake Storage;
  - Then the data is being cleaned/transformed and landed into the BASE layer in ADLS;
  - Finally, data is stored in Data Lakehouse, where it can be used for analysis, visualizations and reporting in future project stages;
  - The project is based on Azure Databricks. All processes are scheduled using Databricks Workflows.
  
  
* Scraping cryptocurrency prices from coinmarketcap.com;


* Scraping stock prices from Yahoo Finance.
