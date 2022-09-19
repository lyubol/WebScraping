# Databricks notebook source
# DBTITLE 1,Notebook Description
# MAGIC %md
# MAGIC The following notebook:
# MAGIC   1. Reads the Noblehire.io Raw data;
# MAGIC      - It contains about four hundred columns, some of which contains nested json objects. Also, it covers different topics;
# MAGIC   2. Cleans and transforms the data;
# MAGIC      - The nested columns are being unpacked and the total columns number goes above one hundred thousand;
# MAGIC   3. Creates thirteen separate DataFrames out of the Raw dataset.
# MAGIC      - Because of the huge amount of columns and due to the data covering different topics, it is splited. 
# MAGIC      
# MAGIC The output of this notebook are the following DataFrames (also saved as .csv files in ADLS)
# MAGIC   - Company Locations Address;
# MAGIC   - Company Locations;
# MAGIC   - Company General;
# MAGIC   - Company Awards;
# MAGIC   - Company Perks;
# MAGIC   - Company Values;
# MAGIC   - Job Requirements;
# MAGIC   - Job Benefits;
# MAGIC   - Job Responsibilities;
# MAGIC   - Job Tools;
# MAGIC   - Job Activities;
# MAGIC   - Job Hiring Process;
# MAGIC   - Posts General.
# MAGIC   
# MAGIC All datasets can be joined together via the post_id or company_id columns.

# COMMAND ----------

# DBTITLE 1,Imports
from datetime import date
import time
import pandas as pd
import numpy as np
from flatten_json import flatten
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Define variables
# Date variables 
current_year = date.today().year
current_month = "0" + str(date.today().month) if len(str(date.today().month)) == 1 else date.today().month
current_day = "0" + str(date.today().day) if len(str(date.today().day)) == 1 else date.today().day

# Base location variables
location_prefix = "/dbfs"
main_path = "/mnt/adlslirkov/it-job-boards/Noblehire.io/base/"
company_locations_address_path = f"companyLocationsAddress/{current_year}/{current_month}/{current_day}/"
company_general_path = f"companyGeneral/{current_year}/{current_month}/{current_day}/"
company_awards_path = f"companyAwards/{current_year}/{current_month}/{current_day}/"
company_perks_path = f"companyPerks/{current_year}/{current_month}/{current_day}/"
company_values_path = f"companyValues/{current_year}/{current_month}/{current_day}/"
company_locations_path = f"companyLocations/{current_year}/{current_month}/{current_day}/"
job_requirements_path = f"jobRequirements/{current_year}/{current_month}/{current_day}/"
job_benefits_path = f"jobBenefits/{current_year}/{current_month}/{current_day}/"
job_responsibilities_path = f"jobResponsibilities/{current_year}/{current_month}/{current_day}/"
job_tools_path = f"jobTools/{current_year}/{current_month}/{current_day}/"
job_activities_path = f"jobActivities/{current_year}/{current_month}/{current_day}/"
job_hiring_process_path = f"jobHiringProcess/{current_year}/{current_month}/{current_day}/"
posts_path = f"posts/{current_year}/{current_month}/{current_day}/"

# COMMAND ----------

# DBTITLE 1,Define functions
# Define a function to flatten complex nested struct type columns
def flatten_struct_df(nested_df):
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']

    flat_df = nested_df.select(flat_cols +
                               [col(nc+'.'+c).alias(nc+'_'+c)
                                for nc in nested_cols
                                for c in nested_df.select(nc+'.*').columns])
    return nested_df


# Define a function to flatten complex nested struct and array type columns
def flatten(df):
  # compute Complex Fields (Lists and Structs) in Schema   
  complex_fields = dict([(field.name, field.dataType) for field in df.schema.fields if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
  
  while len(complex_fields)!=0:
    col_name=list(complex_fields.keys())[0]
    print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))
    
    # if StructType then convert all sub elements to columns.
    # i.e. flatten structs
    if (type(complex_fields[col_name]) == StructType):
      expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
      df=df.select("*", *expanded).drop(col_name)
      
    # if ArrayType then add the Array Elements as Rows using the explode_outer function
    # i.e. explode Arrays
    elif (type(complex_fields[col_name]) == ArrayType):    
      df=df.withColumn(col_name,explode_outer(col_name))
    
    complex_fields = dict([(field.name, field.dataType) for field in df.schema.fields if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
  return df

# COMMAND ----------

# DBTITLE 1,Read Raw data
df = spark.read.format("json").load("/mnt/adlslirkov/it-job-boards/testing/JSON/*.json")

df.display()

# COMMAND ----------

flatten(df.select("id", "activities")).display()

# COMMAND ----------

# DBTITLE 1,Activities
activities_max_size = df.select(max(size(col("activities")))).collect()

results={}
for i in activities_max_size:
  results.update(i.asDict())

df_activities = df.select(col("id"), *[col("activities")[i] for i in range(results["max(size(activities))"])])

df = df.drop("activities")

df_activities = flatten(df_activities)

df_activities.display()

# COMMAND ----------

# DBTITLE 1,Benefits
benefits_max_size = df.select(max(size(col("benefits")))).collect()

results={}
for i in benefits_max_size:
  results.update(i.asDict())

df_benefits = df.select(col("id"), *[col("benefits")[i] for i in range(results["max(size(benefits))"])])

df = df.drop("benefits")

df_benefits = flatten(df_benefits)

df_benefits.display()

# COMMAND ----------

# DBTITLE 1,Company
df_companies = flatten_struct_df(df.select("company").distinct())

df_companies.display()

# COMMAND ----------

# DBTITLE 1,Company
df_companies = flatten(df.select("company"))

df_companies.display()

# COMMAND ----------

rawDf = flatten(df.select("company"))

rawDf.display()

# COMMAND ----------

# df_company_locations = flatten_struct_df(df_companies.select(col("company_locations")[0]))

# df_company_locations.display()

# COMMAND ----------

# d2 = df_company_locations.select(json_tuple(col("company_locations[0]_address"),"address_components","formatted_address","geometry","place_id","types")) \
#     .toDF("address_components","formatted_address","geometry","place_id","types")
# d2.display()

# COMMAND ----------

# df_company_locations_address = flatten_struct_df(df_company_locations.select(col("company_locations[0]_address")[0]))

# df_company_locations_addressdf_company_locations.display()

# COMMAND ----------

# DBTITLE 1,hiringProcessSteps
hiringprocesssteps_max_size = df.select(max(size(col("hiringProcessSteps")))).collect()

results={}
for i in hiringprocesssteps_max_size:
  results.update(i.asDict())

df_hiringprocesssteps = df.select(col("id"), *[col("hiringProcessSteps")[i] for i in range(results["max(size(hiringProcessSteps))"])])

df = df.drop("hiringProcessSteps")

df_hiringprocesssteps.display()

# COMMAND ----------

# DBTITLE 1,Locations
locations_max_size = df.select(max(size(col("locations")))).collect()

results={}
for i in locations_max_size:
  results.update(i.asDict())

df_locations = df.select(col("id"), *[col("locations")[i] for i in range(results["max(size(locations))"])])

df = df.drop("tools")

df_locations.display()

# COMMAND ----------

df_locations.select("locations[0].address").dtypes

# COMMAND ----------

# DBTITLE 1,Tools
tools_max_size = df.select(max(size(col("tools")))).collect()

results={}
for i in tools_max_size:
  results.update(i.asDict())

df_tools = df.select(col("id"), *[col("tools")[i] for i in range(results["max(size(tools))"])])

df = df.drop("tools")

df_tools.display()

# COMMAND ----------

flat_df = flatten_struct_df(df)

flat_df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # OLD CODE BELOW:

# COMMAND ----------

# DBTITLE 1,Read Raw data
# Read the data into Pandas DataFrame (instead of Spark DataFrame) to deal with commas in some columns' text. 
df_posts = pd.read_csv(f"/dbfs/mnt/adlslirkov/it-job-boards/Noblehire.io/raw/posts/{current_year}/{current_month}/{current_day}/noblehireio-posts-{current_year}-{current_month}-{current_day}.csv")

# Remove duplicates if any (based on all columns)
df_posts = df_posts.drop(columns=["Unnamed: 0"]).drop_duplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Company DataFrames and write them to ADLS

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
    df_temp = df_temp.merge(df_posts[["id", "company_id", column.replace("address", "teamSize"), column.replace("address", "founded"), column.replace("address", "comment")]], left_on=df_temp.index, right_on="id")
    # Drop columnsm, which contains only NaN values
    df_temp = df_temp.dropna(axis=0, how="all", subset=[column for column in df_temp.columns if column != "id" and column != "company_id"])
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

# DBTITLE 1,Company Locations
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
    df_temp = df_temp.merge(df_posts[["id", "company_id", column.replace("address", "teamSize"), column.replace("address", "founded"), column.replace("address", "comment")]], left_on=df_temp.index, right_on="id")
    # Drop columnsm, which contains only NaN values
    df_temp = df_temp.dropna(axis=0, how="all", subset=[column for column in df_temp.columns if column != "id" and column != "company_id"])
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
df_company_general["company_overview"] = df_company_general["company_overview"].str.replace("&nbsp;", "").str.replace("amp", "")
df_company_general["company_product"] = df_company_general["company_product"].replace(r'<[^<>]*>', "", regex=True)
df_company_general["description"] = df_company_general["description"].replace(r'<[^<>]*>', "", regex=True)
df_company_general["description"] = df_company_general["description"].str.replace("&nbsp;", "").str.replace("amp", "")
df_company_general["productDescription"] = df_company_general["productDescription"].replace(r'<[^<>]*>', "", regex=True)
df_company_general["productDescription"] = df_company_general["productDescription"].str.replace("&nbsp;", "").str.replace("amp", "")

# Create ADLS location
dbutils.fs.mkdirs(main_path + company_general_path)

# Write to ADLS
df_company_general.to_csv(f"{location_prefix}{main_path}{company_general_path}noblehireio-company_general-{date.today()}.csv")

# COMMAND ----------

# DBTITLE 1,Company Awards
# Create DataFrame to hold company awards information
# df_company_awards = (df_posts[[
#     "company_id",
#     "company_awards_0_title",
#     "company_awards_1_title",
#     "company_awards_2_title",
#     "company_awards_3_title",
#     "company_awards_4_title",
#     "company_awards_5_title",
#     "company_awards_6_title",
#     "company_awards_7_title",
#     "company_awards_8_title"
# ]])

# The number of company awards columns might vary, so we need to dynamically select them instead of hard coding them.
company_awards_columns = [column for column in df_posts.columns if "company_awards" in column and column != "company_awards"]

df_company_awards = (df_posts[[
    "company_id",
    *company_awards_columns
]])

# Create ADLS location
dbutils.fs.mkdirs(main_path + company_awards_path)

# Write to ADLS
df_company_awards.to_csv(f"{location_prefix}{main_path}{company_awards_path}noblehireio-company_awards-{date.today()}.csv")

# COMMAND ----------

# DBTITLE 1,Company Perks
# # Create DataFrame to hold company awards information
# df_company_perks = (df_posts[[
#     "company_id",
#     "company_perks_0_title",
#     "company_perks_0_text",
#     "company_perks_1_title",
#     "company_perks_1_text",
#     "company_perks_2_title",
#     "company_perks_2_text",
#     "company_perks_3_title",
#     "company_perks_3_text",
#     "company_perks_4_title",
#     "company_perks_4_text",
#     "company_perks_5_title",
#     "company_perks_5_text",
#     "company_perks_6_title",
#     "company_perks_6_text",
#     "company_perks_7_title",
#     "company_perks_7_text",
#     "company_perks_8_title",
#     "company_perks_8_text",
#     "company_perks_9_title",
#     "company_perks_9_text",
#     "company_perks_10_title",
#     "company_perks_10_text",
#     "company_perks_11_title",
#     "company_perks_11_text",
#     "company_perks_12_title",
#     "company_perks_12_text",
#     "company_perks_13_title",
#     "company_perks_13_text",
#     "company_perks_14_title",
#     "company_perks_14_text",
#     "company_perks_15_title",
#     "company_perks_15_text",
#     "company_perks_16_title",
#     "company_perks_16_text",
#     "company_perks_17_title",
#     "company_perks_17_text" 
# ]])

# The number of company perks columns might vary, so we need to dynamically select them instead of hard coding them.
company_perks_columns = [column for column in df_posts.columns if "company_perks" in column and column != "company_perks"]

df_company_perks = (df_posts[[
    "company_id",
    *company_perks_columns
]])

# Create ADLS location
dbutils.fs.mkdirs(main_path + company_perks_path)

# Write to ADLS
df_company_perks.to_csv(f"{location_prefix}{main_path}{company_perks_path}noblehireio-company_perks-{date.today()}.csv")

# COMMAND ----------

# DBTITLE 1,Company Values
# # Create DataFrame to hold company awards information
# df_company_values = (df_posts[[
#     "company_id",
#     "company_values_0_title",
#     "company_values_0_text",
#     "company_values_1_title",
#     "company_values_1_text",
#     "company_values_2_title",
#     "company_values_2_text",
#     "company_values_3_title",
#     "company_values_3_text",
#     "company_values_4_title",
#     "company_values_4_text",
#     "company_values_5_title",
#     "company_values_5_text",
#     "company_values_6_title",
#     "company_values_6_text",
#     "company_values_7_title",
#     "company_values_7_text",
#     "company_values_8_title",
#     "company_values_8_text",
#     "company_values_9_title",
#     "company_values_9_text" 
# ]])

# The number of company values columns might vary, so we need to dynamically select them instead of hard coding them.
company_values_columns = [column for column in df_posts.columns if "company_values" in column and column != "company_values"]

df_company_values = (df_posts[[
    "company_id",
    *company_values_columns
]])

# Create ADLS location
dbutils.fs.mkdirs(main_path + company_values_path)

# Write to ADLS
df_company_values.to_csv(f"{location_prefix}{main_path}{company_values_path}noblehireio-company_values-{date.today()}.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Job DataFrames and write them to ADLS

# COMMAND ----------

# Rename id to postId
df_posts = df_posts.rename(columns={"id":"post_id"})

# COMMAND ----------

# DBTITLE 1,Job Requirements
# df_job_requirements = (df_posts[[
#     "post_id",
#     "requirements_0_title",
#     "requirements_1_title",
#     "requirements_2_title",
#     "requirements_3_title",
#     "requirements_4_title",
#     "requirements_5_title",
#     "requirements_6_title",
#     "requirements_7_title",
#     "requirements_8_title",
#     "requirements_9_title",
#     "requirements_10_title",
#     "requirements_11_title",
#     "requirements_12_title",
#     "requirements_13_title",
#     "requirements_14_title",
#     "requirements_15_title",
#     "requirements_16_title",
#     "requirements_17_title"
# ]])

# The number of requirements columns might vary, so we need to dynamically select them instead of hard coding them.
requirements_columns = [column for column in df_posts.columns if "requirements" in column]

df_job_requirements = (df_posts[[
    "post_id",
    *requirements_columns
]])

# Create ADLS location
dbutils.fs.mkdirs(main_path + job_requirements_path)

# Write to ADLS
df_job_requirements.to_csv(f"{location_prefix}{main_path}{job_requirements_path}noblehireio-job_requirements-{date.today()}.csv")

# COMMAND ----------

# DBTITLE 1,Job Benefits
# df_job_benefits = (df_posts[[
#     "post_id",
#     "benefits_0_title",
#     "benefits_1_title",
#     "benefits_2_title",
#     "benefits_3_title",
#     "benefits_4_title",
#     "benefits_5_title",
#     "benefits_6_title",
#     "benefits_7_title",
#     "benefits_8_title",
#     "benefits_9_title",
#     "benefits_10_title",
#     "benefits_11_title",
#     "benefits_12_title",
#     "benefits_13_title",
#     "benefits_14_title",
#     "benefits_15_title",
#     "benefits_16_title",
#     "benefits_17_title",
#     "benefits_18_title",
#     "benefits_19_title",
#     "benefits_20_title",
#     "benefits_21_title",
#     "benefits_22_title",
#     "benefits_23_title",
#     "benefits_24_title",
#     "benefits_25_title",
#     "benefits_26_title",
#     "benefits_27_title",
#     "benefits_28_title",
#     "benefits_29_title",
#     "benefits_30_title"
# ]])

# The number of benefits columns might vary, so we need to dynamically select them instead of hard coding them.
benefits_columns = [column for column in df_posts.columns if "benefits" in column and column != "benefits"]

df_job_benefits = (df_posts[[
    "post_id",
    *benefits_columns
]])

# Create ADLS location
dbutils.fs.mkdirs(main_path + job_benefits_path)

# Write to ADLS
df_job_benefits.to_csv(f"{location_prefix}{main_path}{job_benefits_path}noblehireio-job_benefits-{date.today()}.csv")

# COMMAND ----------

# DBTITLE 1,Job Responsibilities
# df_job_responsibilities = (df_posts[[
#     "post_id",
#     "responsibilities_0_title",
#     "responsibilities_1_title",
#     "responsibilities_2_title",
#     "responsibilities_3_title",
#     "responsibilities_4_title",
#     "responsibilities_5_title",
#     "responsibilities_6_title",
#     "responsibilities_7_title",
#     "responsibilities_8_title",
#     "responsibilities_9_title",
#     "responsibilities_10_title",
#     "responsibilities_11_title",
#     "responsibilities_12_title",
#     "responsibilities_13_title",
#     "responsibilities_14_title",
#     "responsibilities_15_title",
#     "responsibilities_16_title",
#     "responsibilities_17_title",
#     "responsibilities_18_title",
#     "responsibilities_19_title",
#     "responsibilities_20_title"
# ]])

# The number of responsibilities columns might vary, so we need to dynamically select them instead of hard coding them.
responsibilities_columns = [column for column in df_posts.columns if "responsibilities" in column]

df_job_responsibilities = (df_posts[[
    "post_id",
    *responsibilities_columns
]])

# Create ADLS location
dbutils.fs.mkdirs(main_path + job_responsibilities_path)

# Write to ADLS
df_job_responsibilities.to_csv(f"{location_prefix}{main_path}{job_responsibilities_path}noblehireio-job_responsibilities-{date.today()}.csv")

# COMMAND ----------

# DBTITLE 1,Job Tools
# df_job_tools = (df_posts[[
#     "post_id",
#     "tools_0",
#     "tools_1",
#     "tools_2",
#     "tools_3",
#     "tools_4",
#     "tools_5",
#     "tools_6",
#     "tools_7",
#     "tools_8",
#     "tools_9",
#     "tools_10",
#     "tools_11",
#     "tools_12",
#     "tools_13",
#     "tools_14",
#     "tools_15",
#     "tools_16",
#     "tools_17",
#     "tools_18",
#     "tools_19"
# ]])

# The number of tools columns might vary, so we need to dynamically select them instead of hard coding them.
tools_columns = [column for column in df_posts.columns if "tools" in column and column != "tools"]

df_job_tools = (df_posts[[
    "post_id",
    *tools_columns
]])

# Create ADLS location
dbutils.fs.mkdirs(main_path + job_tools_path)

# Write to ADLS
df_job_tools.to_csv(f"{location_prefix}{main_path}{job_tools_path}noblehireio-job_tools-{date.today()}.csv")

# COMMAND ----------

# DBTITLE 1,Job Activities
# df_job_activities = (df_posts[[
#     "post_id",
#     "activities_0_timePercents",
#     "activities_0_title",
#     "activities_1_timePercents",
#     "activities_1_title",
#     "activities_2_timePercents",
#     "activities_2_title",
#     "activities_3_timePercents",
#     "activities_3_title",
#     "activities_4_timePercents",
#     "activities_4_title",
#     "activities_5_timePercents",
#     "activities_5_title",
#     "activities_6_timePercents",
#     "activities_6_title"
# ]])

# The number of activities columns might vary, so we need to dynamically select them instead of hard coding them.
activities_columns = [column for column in df_posts.columns if "activities" in column and column != "activities"]

df_job_activities = (df_posts[[
    "post_id",
    *activities_columns
]])

# Create ADLS location
dbutils.fs.mkdirs(main_path + job_activities_path)

# Write to ADLS
df_job_activities.to_csv(f"{location_prefix}{main_path}{job_activities_path}noblehireio-job_activities-{date.today()}.csv")

# COMMAND ----------

# DBTITLE 1,Job Hiring Process
# df_job_hiring_process = (df_posts[[
#     "post_id",
#     "hiringProcessSteps_0",
#     "hiringProcessSteps_1",
#     "hiringProcessSteps_2",
#     "hiringProcessSteps_3",
#     "hiringProcessSteps_4",
#     "hiringProcessSteps_5"
# ]])

# The number of hiringProcessSteps columns might vary, so we need to dynamically select them instead of hard coding them.
hiring_process_columns = [column for column in df_posts.columns if "hiringProcessSteps" in column and column != "hiringProcessSteps"]

df_job_hiring_process = (df_posts[[
    "post_id",
    *hiring_process_columns
]])

# Create ADLS location
dbutils.fs.mkdirs(main_path + job_hiring_process_path)

# Write to ADLS
df_job_activities.to_csv(f"{location_prefix}{main_path}{job_activities_path}noblehireio-job_hiring_process-{date.today()}.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Posts General DataFrame and write it to ADLS

# COMMAND ----------

# DBTITLE 1,Posts General
# Get all columns from all DataFrames, created above
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

existing_columns = company_general_columns + company_awards_columns + company_perks_columns + company_values_columns + job_requirements + job_benefits + job_responsibilities + job_tools + job_activities + job_hiring_process

# Create posts DataFrame out of all columns that are not included in any of the above DataFrames
df_posts_general = df_posts[[column for column in df_posts.columns if column not in existing_columns]].drop(columns=["locations", "benefits", "activities", "hiringProcessSteps", "tools", "company_perks", "company_values", "company_awards"])

# Replace values
df_posts_general["salaryMin"] = df_posts_general["salaryMin"].replace(0, np.nan)
df_posts_general["salaryMax"] = df_posts_general["salaryMax"].replace(0, np.nan)

# Add ingestion date and source columns
df_posts_general["ingestionDate"] = pd.to_datetime("today").strftime("%Y/%m/%d")
df_posts_general["source"] = "Noblehire.io"

# Rename columns
df_posts_general = df_posts_general.rename(columns={"post_id":"postId", "slug":"jobSlug", "title":"jobTitle"})

# Create ADLS location
dbutils.fs.mkdirs(main_path + posts_path)

# Write to ADLS
df_posts_general.to_csv(f"{location_prefix}{main_path}{posts_path}noblehireio-posts-{date.today()}.csv")
