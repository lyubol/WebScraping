# Databricks notebook source
# DBTITLE 1,Imports
from datetime import date
import time
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Define data
# data = [
#     {
#         "EmployeeId": 1,
#         "FirstName": "Lyubomir",
#         "LastName": "Lirkov",
#         "Position": "S1",
#         "Department": "Delivery",
#         "Salary": 6200
#     },
#     {
#         "EmployeeId": 2,
#         "FirstName": "Kalina",
#         "LastName": "Ivanova",
#         "Position": "C3",
#         "Department": "Delivery",
#         "Salary": 5300
#     },
#     {
#         "EmployeeId": 3,
#         "FirstName": "Alexandar",
#         "LastName": "Alexandrov",
#         "Position": "J3",
#         "Department": "Delivery",
#         "Salary": 3300
#     },
#     {
#         "EmployeeId": 4,
#         "FirstName": "Vladislav",
#         "LastName": "Gankov",
#         "Position": "S2",
#         "Department": "Managed Services",
#         "Salary": 6200
#     },
#     {
#         "EmployeeId": 5,
#         "FirstName": "Ivan",
#         "LastName": "Vazharov",
#         "Position": "P1",
#         "Department": "Delivery",
#         "Salary": 7850
#     }
# ]

data = [
    {
        "EmployeeId": 1,
        "FirstName": "Lyubomir",
        "LastName": "Lirkov",
        "Position": "S1",
        "Department": "Delivery",
        "Salary": 6800
    },
    {
        "EmployeeId": 2,
        "FirstName": "Kalina",
        "LastName": "Ivanova",
        "Position": "C3",
        "Department": "Delivery",
        "Salary": 5300
    },
    {
        "EmployeeId": 3,
        "FirstName": "Alexandar",
        "LastName": "Alexandrov",
        "Position": "J3",
        "Department": "Delivery",
        "Salary": 3300
    },
    {
        "EmployeeId": 4,
        "FirstName": "Vladislav",
        "LastName": "Gankov",
        "Position": "S2",
        "Department": "Delivery",
        "Salary": 6200
    },
    {
        "EmployeeId": 5,
        "FirstName": "Ivan",
        "LastName": "Vazharov",
        "Position": "P3",
        "Department": "Delivery",
        "Salary": 9200
    },
#     {
#         "EmployeeId": 6,
#         "FirstName": "Kalina",
#         "LastName": "Todorova",
#         "Position": "J2",
#         "Department": "Delivery",
#         "Salary": 2800
#     }
#     {
#         "EmployeeId": 7,
#         "FirstName": "Martin",
#         "LastName": "Stoyanov",
#         "Position": "J1",
#         "Department": "Delivery",
#         "Salary": 2000
#     }
]

# COMMAND ----------

# DBTITLE 1,Create Temp Staging Table
# Read Base
df_employees = spark.createDataFrame(data=data, schema="EmployeeId INT, FirstName STRING, LastName STRING, Position STRING, Department STRING, Salary INT")

# Create the Source Data Frame
sourceDF = df_employees
sourceDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Database and Tables

# COMMAND ----------

# DBTITLE 1,Create Database
# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS testing
# MAGIC LOCATION '/mnt/adlslirkov/it-job-boards/testing/delta/'

# COMMAND ----------

# DBTITLE 1,Add SCD Type 2 Columns to Delta Table
# # This command has been ran just once, when the delta table was first created.

df_employees = (
    df_employees
    .withColumn("IsActive", lit(True))
    .withColumn("StartDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("EndDate", lit(None).cast(StringType()))
)

# COMMAND ----------

# DBTITLE 1,Create Delta Table
# # This command has been ran just once, when the delta table was first created.

df_employees.write.format("delta").saveAsTable("testing.employees")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM testing.employees
# MAGIC -- DROP TABLE testing.employees

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
deltaEmployees = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/testing/delta/employees")

targetDF = deltaEmployees.toDF()
targetDF.display()

# COMMAND ----------

# DBTITLE 1,Join source and target
# Rename columns in targetDF, so that we don't need to manually alias them in the join below.
# Since this code will be used for DataFrames with different number of columns and column names, this is the approach that we need to take.
# targetDF = targetDF.toDF(*["target_" + column for column in targetDF.columns])

targetDF = targetDF.filter(col("IsActive") == True).select(*[col for col in targetDF.columns if col not in ["IsActive", "StartDate", "EndDate"]])


joinDF = (
    sourceDF
    .join(
        targetDF, 
        (sourceDF.EmployeeId == targetDF.EmployeeId),
#         & (targetDF.IsActive == "true"),
        "outer"
    )
    .select(
        sourceDF["*"],
        targetDF.EmployeeId.alias("target_EmployeeId"),
        targetDF.FirstName.alias("target_FirstName"),
        targetDF.La.where(col("Link") == "https://dev.bg/company/jobads/sourcelab-mid-front-end-developer/")stName.alias("target_LastName"),
        targetDF.Position.alias("target_Position"),
        targetDF.Department.alias("target_Department"),
        targetDF.Salary.alias("target_Salary"),
    )
)

joinDF.display()

# COMMAND ----------

# DBTITLE 1,Hash source and target columns and compare them
filterDF = joinDF.filter(xxhash64(*[col for col in joinDF.columns if col.startswith("target") == False and "IngestionDate" not in col]) != xxhash64(*[col for col in joinDF.columns if col.startswith("target") == True and "IngestionDate" not in col])).withColumn("MergeKey", col("target_EmployeeId"))

filterDF.display()

# COMMAND ----------

# DBTITLE 1,Add MergeKey and set it to null where Id is not null
dummyDF = filterDF.filter(col("target_EmployeeId").isNotNull()).withColumn("MergeKey", lit(None))

dummyDF.display()

# COMMAND ----------

# DBTITLE 1,Union DFs
scdDF = filterDF.union(dummyDF)

scdDF.display()

# COMMAND ----------

# DBTITLE 1,Merge
(deltaEmployees.alias("target")
 .merge(
     scdDF.alias("source"),
     "target.EmployeeId = source.MergeKey"
 )
 .whenMatchedUpdate(set = 
    {
        "IsActive": "'False'", 
        "EndDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
    }
 )
 .whenNotMatchedInsert(
     condition = "source.EmployeeId IS NOT NULL",
     values =
        {
             'EmployeeId': 'source.EmployeeId',
             'FirstName': 'source.FirstName',
             'LastName': 'source.LastName',
             'Position': 'source.Position',
             'Department': 'source.Department',
             'Salary': 'source.Salary',
             'IsActive': "'True'",
             'StartDate': 'current_timestamp'
        }
 )
 .execute()
)

# COMMAND ----------

# DBTITLE 1,Check Delta Table History
deltaEmployees.history().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM testing.employees
# MAGIC WHERE IsActive = True

# COMMAND ----------


