# Bulgarian IT Job Boards

Project Developer: Lyubomir Lirkov
Last Updated Date: 2023/03/18
Infrastructure Components: Azure Databricks, Azure Data Lake Storage
Languages Used: Python (PySpark), SQL

## 1. Overview

This is a data engineering project that is based on scraping data from Bulgarian IT job boards. The idea behind the project is to give overview of the Bulgarian IT job market, by scraping data from the most popular IT job boards (currently - noblehire.io, dev.bg, zaplata.bg). Data is scraped daily using python scripts. Once scraped the data is stored into an Azure Data Lake Storage. Then it is processed further to another layer, where cleaning and transformation logic is applied. Finally, dimensions and facts are build on top of it and a dimensional data model is created. 


## 2. Components

### 2.1 Storage
Azure Data Lake Storage container is used for storing data. It has the following layers:
  - RAW - data is landed in source format
  - BASE - data is cleaned and transformations are applied
  - WAREHOUSE - facts and dimension tables, SQL objects definitions

### 2.2 Processing
The project is entirely based on Databricks notebooks. These are the main processing components:
  - main.py - This file contains the scraping logic for each of the source systems;
  - Source to Raw Notebooks - This folder contains a notebook for each source system, which scrapes the data and stores it in source format, into the RAW layer of the data lake.
  - Raw to Base Notebooks - This folder contains transformation notebooks, which move the data from the RAW to the BASE layer, for each source system;
  - Warehouse - This folder contains all notebooks that are required in order to build the dimensional model. It consists of dimension and fact notebooks. Also, all SQL objects definitions are stored here.

### 2.3 Orchestration
Since the project is entirely based on Azure Databricks, the orchestration is done via scheduled Workflows jobs. The file 'WorkflowsOrchestration.json holds the orchestration logic.

