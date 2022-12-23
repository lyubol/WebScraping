# Databricks notebook source
# DBTITLE 1,Imports
from datetime import date
import time
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Define variables
# Date variables 
current_year = date.today().year
current_month = "0" + str(date.today().month) if len(str(date.today().month)) == 1 else date.today().month
current_day = "0" + str(date.today().day) if len(str(date.today().day)) == 1 else date.today().day

# Base location variables
location_prefix = "/dbfs"
main_path = "/mnt/adlslirkov/it-job-boards/DEV.bg/base/"
company_path = f"company/{current_year}/{current_month}/{current_day}/"

# COMMAND ----------

# DBTITLE 1,Create Temp Staging Table
# Read Base
df_company = spark.read.format("parquet").load(main_path + company_path)

# Create the Source Data Frame
sourceDF = df_company
sourceDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Database and Tables

# COMMAND ----------

# DBTITLE 1,Create Database
# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS jobposts_devbg
# MAGIC COMMENT 'This database holds job posts data coming from Dev.bg'
# MAGIC LOCATION '/mnt/adlslirkov/it-job-boards/DEV.bg/delta/'

# COMMAND ----------

# DBTITLE 1,Add SCD Type 2 Columns to Delta Table
# # This command has been ran just once, when the delta table was first created.

# df_company = (
#     df_company
#     .withColumn("IsActive", lit(True))
#     .withColumn("StartDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
#     .withColumn("EndDate", lit(None).cast(StringType()))
# )

# COMMAND ----------

# DBTITLE 1,Create Delta Table
# # This command has been ran just once, when the delta table was first created.

# df_company.write.format("delta").saveAsTable("jobposts_devbg.company")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM jobposts_devbg.company
# MAGIC -- DROP TABLE jobposts_devbg.company

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
deltaCompany = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/DEV.bg/delta/company")

targetDF = deltaCompany.toDF()
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
        (sourceDF.Company == targetDF.Company),
#         & (targetDF.IsActive == "true"),
        "leftouter"
    )
    .select(
        sourceDF["*"],
        targetDF.Company.alias("target_Company"),
        targetDF.Locationburgas.alias("target_Locationburgas"),
        targetDF.Locationplovdiv.alias("target_Locationplovdiv"),
        targetDF.Locationruse.alias("target_Locationruse"),
        targetDF.Locationsofia.alias("target_Locationsofia"),
        targetDF.Locationvarna.alias("target_Locationvarna"),
        targetDF.V_Balgaria.alias("target_V_Balgaria"),
        targetDF.V_Chuzhbina.alias("target_V_Chuzhbina"),
        targetDF["Employees_1_9"].alias("target_Employees_1_9"),
        targetDF["Employees_10_30"].alias("target_Employees_10_30"),
        targetDF["Employees_31_70"].alias("target_Employees_31_70"),
        targetDF["Employees_70"].alias("target_Employees_70"),
        targetDF.It_Konsultirane.alias("target_It_Konsultirane"),
        targetDF.Produktovi_Kompanii.alias("target_Produktovi_Kompanii"),
        targetDF.Survis_Kompanii.alias("target_Survis_Kompanii"),
        targetDF.Vnedrjavane_Na_Softuerni_Sistemi.alias("target_Vnedrjavane_Na_Softuerni_Sistemi"),
        targetDF["Dni_20"].alias("target_Dni_20"),
        targetDF["Dni_21_25"].alias("target_Dni_21_25"),
        targetDF["Dni_25"].alias("target_Dni_25"),
        targetDF.Chastichno_Guvkavo.alias("target_Chastichno_Guvkavo"),
        targetDF.Fiksirano.alias("target_Fiksirano"),
        targetDF.Iztsyalo_Guvkavo.alias("target_Iztsyalo_Guvkavo"),
        targetDF.Source.alias("target_Source"),
        targetDF.IngestionDate.alias("target_IngestionDate")
    )
)

joinDF.display()

# COMMAND ----------

# DBTITLE 1,Hash source and target columns and compare them
filterDF = joinDF.filter(xxhash64(*[col for col in joinDF.columns if col.startswith("target") == False and "IngestionDate" not in col]) != xxhash64(*[col for col in joinDF.columns if col.startswith("target") == True and "IngestionDate" not in col])).withColumn("MergeKey", col("Company"))

filterDF.display()

# COMMAND ----------

# DBTITLE 1,Add MergeKey and set it to null where Id is not null
dummyDF = filterDF.filter(col("target_Company").isNotNull()).withColumn("MergeKey", lit(None))

dummyDF.display()

# COMMAND ----------

# DBTITLE 1,Union DFs
scdDF = filterDF.union(dummyDF)

scdDF.display()

# COMMAND ----------

# DBTITLE 1,Create Dictionary which will be used in the Merge Command
columns_dict = {col: "source." + col for col in df_company.columns}
columns_dict["IsActive"] = "'True'"
columns_dict["StartDate"] = "current_timestamp"
# columns_dict["EndDate"] = """to_date('9999-12-31 00:00:00.0000', 'MM-dd-yyyy HH:mm:ss')"""

columns_dict

# COMMAND ----------

# DBTITLE 1,Merge
(deltaCompany.alias("target")
 .merge(
     scdDF.alias("source"),
     "target.Company = source.MergeKey"
 )
 .whenMatchedUpdate(set = 
    {
        "Source": "source.Source",
        "IsActive": "'False'", 
        "EndDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
    }
 )
 .whenNotMatchedInsert(values =
        {
             'Company': 'source.Company',
             'Locationburgas': 'source.Locationburgas',
             'Locationplovdiv': 'source.Locationplovdiv',
             'Locationruse': 'source.Locationruse',
             'Locationsofia': 'source.Locationsofia',
             'Locationvarna': 'source.Locationvarna',
             'V_Balgaria': 'source.V_Balgaria',
             'V_Chuzhbina': 'source.V_Chuzhbina',
             'Employees_1_9': 'source.Employees_1_9',
             'Employees_10_30': 'source.Employees_10_30',
             'Employees_31_70': 'source.Employees_31_70',
             'Employees_70': 'source.Employees_70',
             'It_Konsultirane': 'source.It_Konsultirane',
             'Produktovi_Kompanii': 'source.Produktovi_Kompanii',
             'Survis_Kompanii': 'source.Survis_Kompanii',
             'Vnedrjavane_Na_Softuerni_Sistemi': 'source.Vnedrjavane_Na_Softuerni_Sistemi',
             'Dni_20': 'source.Dni_20',
             'Dni_21_25': 'source.Dni_21_25',
             'Dni_25': 'source.Dni_25',
             'Chastichno_Guvkavo': 'source.Chastichno_Guvkavo',
             'Fiksirano': 'source.Fiksirano',
             'Iztsyalo_Guvkavo': 'source.Iztsyalo_Guvkavo',
             'Source': 'source.Source',
             'IngestionDate': 'source.IngestionDate',
             'IsActive': "'True'",
             'StartDate': 'current_timestamp'
        }
 )
 .execute()
)

# COMMAND ----------

# DBTITLE 1,Check Delta Table History
deltaCompany.history().display()
