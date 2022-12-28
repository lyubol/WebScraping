# Databricks notebook source
# MAGIC %run "lirkov/IT Job Boards/Main"

# COMMAND ----------

# DBTITLE 1,Define variables
# Date variables 
current_year = date.today().year
current_month = "0" + str(date.today().month) if len(str(date.today().month)) == 1 else date.today().month
current_day = "0" + str(date.today().day) if len(str(date.today().day)) == 1 else date.today().day

# Raw and Base location variables
location_prefix = "/dbfs"
main_path_raw = "/mnt/adlslirkov/it-job-boards/DEV.bg/raw/"
main_path_base = "/mnt/adlslirkov/it-job-boards/DEV.bg/base/"
posts_path = f"posts/{current_year}/{current_month}/{current_day}/"
descriptions_path = f"descriptions/{current_year}/{current_month}/{current_day}/"
company_path = f"company/{current_year}/{current_month}/{current_day}/" # Base only
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

# DBTITLE 1,Read Raw data
df_jobposts = spark.read.format("parquet").load(f"{main_path_raw + posts_path}")
# df_jobdescriptions = spark.read.format("parquet").load(f"{main_path_raw + descriptions_path}")
df_companylocations = spark.read.format("parquet").load(f"{main_path_raw + company_location_path}")
df_companyheadquarters = spark.read.format("parquet").load(f"{main_path_raw + company_headquarters_path}")
df_employeescount = spark.read.format("parquet").load(f"{main_path_raw + company_employees_count_path}")
df_companyactivities = spark.read.format("parquet").load(f"{main_path_raw + company_activity_path}")
df_companypaidleaves = spark.read.format("parquet").load(f"{main_path_raw + company_paid_leave_path}")
df_companyworkhours = spark.read.format("parquet").load(f"{main_path_raw + company_work_hours_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Company related data

# COMMAND ----------

# DBTITLE 1,Pivot company DataFrames
df_companylocations_pivoted = df_companylocations.groupBy("Company").pivot("Location").count()
df_companyheadquarters_pivoted = df_companyheadquarters.groupBy("Company").pivot("Headquarter").count()
df_employeescount_pivoted = df_employeescount.groupBy("Company").pivot("EmployeesCount").count()
df_companyactivities_pivoted = df_companyactivities.groupBy("Company").pivot("Activity").count()
df_companypaidleaves_pivoted = df_companypaidleaves.groupBy("Company").pivot("PaidLeave").count()
df_companyworkhours_pivoted = df_companyworkhours.groupBy("Company").pivot("WorkHours").count()

# df_companylocations_pivoted.display()
# df_companyheadquarters_pivoted.display()
# df_employeescount_pivoted.display()
# df_companyactivities_pivoted.display()
# df_companypaidleaves_pivoted.display()
# df_companyworkhours_pivoted.display()

# COMMAND ----------

# DBTITLE 1,Join pivoted company DataFrames
df_company = (
    df_companylocations_pivoted
    .join(df_companyheadquarters_pivoted, "Company", how="fullouter")
    .join(df_employeescount_pivoted, "Company", how="fullouter")
    .join(df_companyactivities_pivoted, "Company", how="fullouter")
    .join(df_companypaidleaves_pivoted, "Company", how="fullouter")
    .join(df_companyworkhours_pivoted, "Company", how="fullouter")
)

df_company.display()

# COMMAND ----------

# DBTITLE 1,Adjust column names
df_company = df_company.toDF(*[c.replace("-", "_").title() for c in df_company.columns])

# COMMAND ----------

# DBTITLE 1,Add date and source columns
df_company = (df_company
  .withColumn("Source", lit("Dev.bg"))
  .withColumn("IngestionDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
)

df_company.display()

# COMMAND ----------

# DBTITLE 1,Write to Base
df_company.write.format("parquet").mode("overwrite").save(f"{main_path_base + company_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Job posts and descriptions related data

# COMMAND ----------

# DBTITLE 1,Add date and source columns
df_jobposts = (df_jobposts
  .withColumn("Source", lit("Dev.bg"))
  .withColumn("IngestionDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
).distinct()

df_jobposts.display()

# COMMAND ----------

# df_jobposts.select(split("uploaded", " ").getItem(1)).distinct().display()

# COMMAND ----------

# DBTITLE 1,Add date and source columns
# df_jobdescriptions = (df_jobdescriptions
#   .withColumn("Source", lit("Dev.bg"))
#   .withColumn("IngestionDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
# )

# df_jobdescriptions.display()

# COMMAND ----------

# DBTITLE 1,Write to Base
df_jobposts.distinct().write.format("parquet").mode("overwrite").save(f"{main_path_base + posts_path}")
# df_jobdescriptions.write.format("parquet").mode("overwrite").save(f"{main_path_base + descriptions_path}")
