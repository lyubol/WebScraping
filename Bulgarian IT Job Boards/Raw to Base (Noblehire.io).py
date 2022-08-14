# Databricks notebook source
# DBTITLE 1,Imports
from datetime import date
import time
import pandas as pd
import numpy as np
from flatten_json import flatten

# COMMAND ----------

# DBTITLE 1,Set options
pd.options.mode.chained_assignment = None  # default='warn'
pd.set_option('display.max_columns', None) 

# COMMAND ----------

# DBTITLE 1,Define variables
# Base location variables
location_prefix = "/dbfs"
main_path = "/mnt/adlslirkov/it-job-boards/Noblehire.io/base/"
company_locations_address_path = f"companyLocationsAddress/{date.today().year}/{date.today().month}/{date.today().day}/"
company_general_path = f"companyGeneral/{date.today().year}/{date.today().month}/{date.today().day}/"
company_awards_path = f"companyAwards/{date.today().year}/{date.today().month}/{date.today().day}/"
company_perks_path = f"companyPerks/{date.today().year}/{date.today().month}/{date.today().day}/"
company_values_path = f"companyValues/{date.today().year}/{date.today().month}/{date.today().day}/"
company_locations_path = f"companyLocations/{date.today().year}/{date.today().month}/{date.today().day}/"
job_requirements_path = f"jobRequirements/{date.today().year}/{date.today().month}/{date.today().day}/"
job_benefits_path = f"jobBenefits/{date.today().year}/{date.today().month}/{date.today().day}/"
job_responsibilities_path = f"jobResponsibilities/{date.today().year}/{date.today().month}/{date.today().day}/"
job_tools_path = f"jobTools/{date.today().year}/{date.today().month}/{date.today().day}/"
job_activities_path = f"jobActivities/{date.today().year}/{date.today().month}/{date.today().day}/"
job_hiring_process_path = f"jobHiringProcess/{date.today().year}/{date.today().month}/{date.today().day}/"

# COMMAND ----------

# DBTITLE 1,Define functions
# Define a function to add prefix to each column name
def addColumnNamePrefix(DataFrame, prefix):
    new_column_names = []
    for column in DataFrame.columns:
        new_column_names.append(prefix + column)
    return new_column_names
  
    
# Define function to unpack complex dictionary
# Add id to be able to match this data back to the original dataframe, later.
def addressToDF(DataFrame, id_column, address_column):
    main = {}
    temp = {}
    for company_id, address in zip(DataFrame[id_column], DataFrame[address_column]):
        for k, v in flatten(address).items():
            temp[k] = v
        main[company_id] = temp
        temp = {}
    return main


# Define function to apply transformation logic and convert dictionary into a DataFrame
# The function is used for company_location_address columns
def transformCompanyLocationColumns(DataFrame, column):
    # Fill Na values to apply eval()
    DataFrame[column] = DataFrame[column].fillna("{}")
    # Apply eval to turn strings in dictionaries
    DataFrame[column] = DataFrame[column].apply(lambda x: eval(str(x)))
    # Create DataFrame, transpose and convert data types
    df_temp = pd.DataFrame.from_dict(addressToDF(df_posts, "id", column)).transpose().convert_dtypes()
    # Since all location DataFrames have identical column names, we are adding the location number as a prefix to each name
    df_temp.columns = addColumnNamePrefix(df_temp, column.replace("address", ""))
    return df_temp

# COMMAND ----------

# DBTITLE 1,Read Raw data
# Read the data into Pandas DataFrame (instead of Spark DataFrame) to deal with commas in some columns' text. 
df_posts = pd.read_csv("/dbfs/mnt/adlslirkov/it-job-boards/Noblehire.io/raw/posts/2022/8/14/noblehireio-posts-2022-08-14.csv")

# Remove duplicates if any (based on all columns)
df_posts = df_posts.drop(columns=["Unnamed: 0"]).drop_duplicates()

# COMMAND ----------

# DBTITLE 1,Company Locations Address
# Create ADLS location
dbutils.fs.mkdirs(main_path + company_locations_address_path)


# Create list with all company_location columns in the main DataFrame
company_location_address_cols = [column for column in df_posts.columns if "company_locations" in column and "address" in column]


# Apply the transformation logic to each column and write to ADLS
for column in company_location_address_cols:
    # Set id column
    id_column = column.replace("address", "id")
    # Flatten each company_location_address column and return it as a DataFrame
    df_temp = transformCompanyLocationColumns(df_posts, column)
    # Bring any other related columns from the main DataFrame to the company location address DataFrame
    df_temp = df_temp.merge(df_posts[["id", column.replace("address", "teamSize"), column.replace("address", "founded"), column.replace("address", "comment")]], left_on=df_temp.index, right_on="id")
    # Drop columnsm, which contains only NaN values
    df_temp = df_temp.dropna(axis=0, how="all", subset=[column for column in df_temp.columns if column != "id"])
    # Save to companyLocationAddress folder in ADLS
    # Set file name 
    save_to_file_name = f"noblehireio-{column}-{date.today()}.csv"
    # Save to ADLS
    df_temp.to_csv(location_prefix + main_path + company_locations_address_path + save_to_file_name)
    # Drop original columns from the main DataFrame
    df_posts = df_posts.drop(columns=[column, column.replace("address", "id"), column.replace("address", "teamSize"), column.replace("address", "founded"), column.replace("address", "comment")])
    # Print file location and name
    print(f"File saved to location: {location_prefix + main_path + company_locations_address_path + save_to_file_name}")

# COMMAND ----------

# DBTITLE 1,Locations
# Create ADLS location
dbutils.fs.mkdirs(main_path + company_locations_path)


# Create list with all company_location columns in the main DataFrame
company_locations_cols = [column for column in df_posts.columns if "locations" in column and "address" in column]


# Apply the transformation logic to each column and write to ADLS
for column in company_locations_cols:
    # Set id column
    id_column = column.replace("address", "id")
    # Flatten each company_location_address column and return it as a DataFrame
    df_temp = transformCompanyLocationColumns(df_posts, column)
    # Bring any other related columns from the main DataFrame to the company location address DataFrame
    df_temp = df_temp.merge(df_posts[["id", column.replace("address", "teamSize"), column.replace("address", "founded"), column.replace("address", "comment")]], left_on=df_temp.index, right_on="id")
    # Drop columnsm, which contains only NaN values
    df_temp = df_temp.dropna(axis=0, how="all", subset=[column for column in df_temp.columns if column != "id"])
    # Save to companyLocationAddress folder in ADLS
    # Set file name 
    save_to_file_name = f"noblehireio-{column}-{date.today()}.csv"
    # Save to ADLS
    df_temp.to_csv(location_prefix + main_path + company_locations_path + save_to_file_name)
    # Drop original columns from the main DataFrame
    df_posts = df_posts.drop(columns=[column, column.replace("address", "id"), column.replace("address", "teamSize"), column.replace("address", "founded"), column.replace("address", "comment")])
    # Print file location and name
    print(f"File saved to location: {location_prefix + main_path + company_locations_path + save_to_file_name}")

# COMMAND ----------

# DBTITLE 1,Company General
# Create DataFrame to hold general company information
df_company_general = (df_posts[[
    "company_id",
    "company_slug",
    "company_brand",
    "company_overview",
    "company_product",
    "company_public",
    "description",
    "productDescription"
]])

# Apply transformations to remove html tags from text columns
df_company_general["company_overview"] = df_company_general["company_overview"].replace(r'<[^<>]*>', "", regex=True)
df_company_general["company_product"] = df_company_general["company_product"].replace(r'<[^<>]*>', "", regex=True)
df_company_general["description"] = df_company_general["description"].replace(r'<[^<>]*>', "", regex=True)
df_company_general["productDescription"] = df_company_general["productDescription"].replace(r'<[^<>]*>', "", regex=True)

# Create ADLS location
dbutils.fs.mkdirs(main_path + company_general_path)

# Write to ADLS
df_company_general.to_csv(f"{location_prefix}{main_path}{company_general_path}noblehireio-company_general-{date.today()}.csv")

# COMMAND ----------

# DBTITLE 1,Company Awards
# Create DataFrame to hold company awards information
df_company_awards = (df_posts[[
    "company_id",
    "company_awards_0_title",
    "company_awards_1_title",
    "company_awards_2_title",
    "company_awards_3_title",
    "company_awards_4_title",
    "company_awards_5_title",
    "company_awards_6_title",
    "company_awards_7_title",
    "company_awards_8_title"
]])

# Create ADLS location
dbutils.fs.mkdirs(main_path + company_awards_path)

# Write to ADLS
df_company_awards.to_csv(f"{location_prefix}{main_path}{company_awards_path}noblehireio-company_awards-{date.today()}.csv")

# COMMAND ----------

# DBTITLE 1,Company Perks
# Create DataFrame to hold company awards information
df_company_perks = (df_posts[[
    "company_id",
    "company_perks_0_title",
    "company_perks_0_text",
    "company_perks_1_title",
    "company_perks_1_text",
    "company_perks_2_title",
    "company_perks_2_text",
    "company_perks_3_title",
    "company_perks_3_text",
    "company_perks_4_title",
    "company_perks_4_text",
    "company_perks_5_title",
    "company_perks_5_text",
    "company_perks_6_title",
    "company_perks_6_text",
    "company_perks_7_title",
    "company_perks_7_text",
    "company_perks_8_title",
    "company_perks_8_text",
    "company_perks_9_title",
    "company_perks_9_text",
    "company_perks_10_title",
    "company_perks_10_text",
    "company_perks_11_title",
    "company_perks_11_text",
    "company_perks_12_title",
    "company_perks_12_text",
    "company_perks_13_title",
    "company_perks_13_text",
    "company_perks_14_title",
    "company_perks_14_text",
    "company_perks_15_title",
    "company_perks_15_text",
    "company_perks_16_title",
    "company_perks_16_text",
    "company_perks_17_title",
    "company_perks_17_text" 
]])

# Create ADLS location
dbutils.fs.mkdirs(main_path + company_perks_path)

# Write to ADLS
df_company_perks.to_csv(f"{location_prefix}{main_path}{company_perks_path}noblehireio-company_perks-{date.today()}.csv")

# COMMAND ----------

# DBTITLE 1,Company Values
# Create DataFrame to hold company awards information
df_company_values = (df_posts[[
    "company_id",
    "company_values_0_title",
    "company_values_0_text",
    "company_values_1_title",
    "company_values_1_text",
    "company_values_2_title",
    "company_values_2_text",
    "company_values_3_title",
    "company_values_3_text",
    "company_values_4_title",
    "company_values_4_text",
    "company_values_5_title",
    "company_values_5_text",
    "company_values_6_title",
    "company_values_6_text",
    "company_values_7_title",
    "company_values_7_text",
    "company_values_8_title",
    "company_values_8_text",
    "company_values_9_title",
    "company_values_9_text" 
]])

# Create ADLS location
dbutils.fs.mkdirs(main_path + company_values_path)

# Write to ADLS
df_company_values.to_csv(f"{location_prefix}{main_path}{company_values_path}noblehireio-company_values-{date.today()}.csv")

# COMMAND ----------

# DBTITLE 1,Job requirements
df_job_requirements = (df_posts[[
    "company_id",
    "requirements_0_title",
    "requirements_1_title",
    "requirements_2_title",
    "requirements_3_title",
    "requirements_4_title",
    "requirements_5_title",
    "requirements_6_title",
    "requirements_7_title",
    "requirements_8_title",
    "requirements_9_title",
    "requirements_10_title",
    "requirements_11_title",
    "requirements_12_title",
    "requirements_13_title",
    "requirements_14_title",
    "requirements_15_title",
    "requirements_16_title",
    "requirements_17_title"
]])

# Create ADLS location
dbutils.fs.mkdirs(main_path + job_requirements_path)

# Write to ADLS
df_job_requirements.to_csv(f"{location_prefix}{main_path}{job_requirements_path}noblehireio-job_requirements-{date.today()}.csv")

# COMMAND ----------

# DBTITLE 1,Job benefits
df_job_benefits = (df_posts[[
    "company_id",
    "benefits_0_title",
    "benefits_1_title",
    "benefits_2_title",
    "benefits_3_title",
    "benefits_4_title",
    "benefits_5_title",
    "benefits_6_title",
    "benefits_7_title",
    "benefits_8_title",
    "benefits_9_title",
    "benefits_10_title",
    "benefits_11_title",
    "benefits_12_title",
    "benefits_13_title",
    "benefits_14_title",
    "benefits_15_title",
    "benefits_16_title",
    "benefits_17_title",
    "benefits_18_title",
    "benefits_19_title",
    "benefits_20_title",
    "benefits_21_title",
    "benefits_22_title",
    "benefits_23_title",
    "benefits_24_title",
    "benefits_25_title",
    "benefits_26_title",
    "benefits_27_title",
    "benefits_28_title",
    "benefits_29_title",
    "benefits_30_title"
]])

# Create ADLS location
dbutils.fs.mkdirs(main_path + job_benefits_path)

# Write to ADLS
df_job_benefits.to_csv(f"{location_prefix}{main_path}{job_benefits_path}noblehireio-job_benefits-{date.today()}.csv")

# COMMAND ----------

# DBTITLE 1,Job responsibilities
df_job_responsibilities = (df_posts[[
    "company_id",
    "responsibilities_0_title",
    "responsibilities_1_title",
    "responsibilities_2_title",
    "responsibilities_3_title",
    "responsibilities_4_title",
    "responsibilities_5_title",
    "responsibilities_6_title",
    "responsibilities_7_title",
    "responsibilities_8_title",
    "responsibilities_9_title",
    "responsibilities_10_title",
    "responsibilities_11_title",
    "responsibilities_12_title",
    "responsibilities_13_title",
    "responsibilities_14_title",
    "responsibilities_15_title",
    "responsibilities_16_title",
    "responsibilities_17_title",
    "responsibilities_18_title",
    "responsibilities_19_title",
    "responsibilities_20_title"
]])

# Create ADLS location
dbutils.fs.mkdirs(main_path + job_responsibilities_path)

# Write to ADLS
df_job_responsibilities.to_csv(f"{location_prefix}{main_path}{job_responsibilities_path}noblehireio-job_responsibilities-{date.today()}.csv")

# COMMAND ----------

# DBTITLE 1,Job tools
df_job_tools = (df_posts[[
    "company_id",
    "tools_0",
    "tools_1",
    "tools_2",
    "tools_3",
    "tools_4",
    "tools_5",
    "tools_6",
    "tools_7",
    "tools_8",
    "tools_9",
    "tools_10",
    "tools_11",
    "tools_12",
    "tools_13",
    "tools_14",
    "tools_15",
    "tools_16",
    "tools_17",
    "tools_18",
    "tools_19"
]])

# Create ADLS location
dbutils.fs.mkdirs(main_path + job_tools_path)

# Write to ADLS
df_job_tools.to_csv(f"{location_prefix}{main_path}{job_tools_path}noblehireio-job_tools-{date.today()}.csv")

# COMMAND ----------

# DBTITLE 1,Job activities
df_job_activities = (df_posts[[
    "company_id",
    "activities_0_title",
    "activities_1_timePercents",
    "activities_1_title",
    "activities_2_timePercents",
    "activities_2_title",
    "activities_3_timePercents",
    "activities_3_title",
    "activities_4_timePercents",
    "activities_4_title",
    "activities_5_timePercents",
    "activities_5_title",
    "activities_6_timePercents",
    "activities_6_title"
]])

# Create ADLS location
dbutils.fs.mkdirs(main_path + job_activities_path)

# Write to ADLS
df_job_activities.to_csv(f"{location_prefix}{main_path}{job_activities_path}noblehireio-job_activities-{date.today()}.csv")

# COMMAND ----------

# DBTITLE 1,Job hiring process
df_job_hiring_process = (df_posts[[
    "company_id",
    "hiringProcessSteps_0",
    "hiringProcessSteps_1",
    "hiringProcessSteps_2",
    "hiringProcessSteps_3",
    "hiringProcessSteps_4",
    "hiringProcessSteps_5"
]])

# Create ADLS location
dbutils.fs.mkdirs(main_path + job_hiring_process_path)

# Write to ADLS
df_job_activities.to_csv(f"{location_prefix}{main_path}{job_activities_path}noblehireio-job_hiring_process-{date.today()}.csv")

# COMMAND ----------

# DBTITLE 1,Posts general
company_general_columns = df_company_general.columns.to_list()
company_awards_columns = df_company_awards.columns.to_list()
company_perks_columns = df_company_perks.columns.to_list()
company_values_columns = df_company_values.columns.to_list()
job_requirements = df_job_requirements.columns.to_list()
job_benefits = df_job_benefits.columns.to_list()
job_responsibilities = df_job_responsibilities.columns.to_list()
job_tools = df_job_tools.columns.to_list()
job_activities = df_job_activities.columns.to_list()
job_hiring_process = df_job_hiring_process.columns.to_list()

df_posts[[column for column in df_posts.columns if column not in company_general_columns and column not in company_awards_columns and column not in company_perks_columns and column not in company_values_columns and column not in job_requirements and column not in job_benefits and column not in job_responsibilities and column not in job_tools and column not in job_activities and column not in job_hiring_process]]

# df['date'] = pd.to_datetime(df['date'],unit='s')
