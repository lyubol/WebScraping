# Databricks notebook source
# MAGIC %run "lirkov/IT Job Boards/Main"

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import Window
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Define variables
# Date variables 
current_year = date.today().year
current_month = "0" + str(date.today().month) if len(str(date.today().month)) == 1 else date.today().month
current_day = "0" + str(date.today().day) if len(str(date.today().day)) == 1 else date.today().day

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create DimLocations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data

# COMMAND ----------

# Read base
df_locations_noblehire_base = spark.read.format("parquet").load(f"/mnt/adlslirkov/it-job-boards/Noblehire.io/base/companyLocations/{current_year}/{current_month}/{current_day}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data

# COMMAND ----------

# DBTITLE 1,Define functions
# define function to extract city and country from address string
def getAddress(address):
    if address != None:
        return ",".join(address.split(",")[-2:])

# define function to spread team size values into buckets
def teamSizeBuckets(teamsize):
    if teamsize == None:
        return "Other"
    elif "-" in teamsize:
        return teamsize.strip().replace(" ", "")
    elif teamsize.isnumeric():
        teamsize = int(teamsize)
        if teamsize >= 1 and teamsize <= 10:
            return "2-10"
        elif teamsize >= 11 and teamsize <= 50:
            return "11-50"
        elif teamsize >= 51 and teamsize <= 200:
            return "51-200"
        elif teamsize >= 201 and teamsize <= 1000:
            return "201-1000"
        else:
            return "1000+"
    else:
        return "Not Available"

# COMMAND ----------

# DBTITLE 1,Register functions
# register function
getAddress_udf = udf(getAddress)

# register function
teamSizeBuckets_udf = udf(teamSizeBuckets)

# COMMAND ----------

# Select and rename columns
df_locations_noblehire_enriched = (
    df_locations_noblehire_base
      .select(
        col("id").alias("LocationId"),
        col("companyId").alias("CompanyId"),
        col("Source").alias("SourceSystem"),
        col("locations_0_comment").alias("LocationComment0"),
        col("locations_0_founded").alias("LocationFounded0").cast("int"),
        col("locations_0_teamSize").alias("LocationTeamSize0"),
        col("locations_0_address_formatted_address").alias("LocationAddress0"),
        col("locations_0_address_location_type").alias("LocationType0"),
        col("locations_0_address_latitude").alias("Latitude0"),
        col("locations_0_address_longitude").alias("Longitude0"),
        col("locations_1_comment").alias("LocationComment1"),
        col("locations_1_founded").alias("LocationFounded1").cast("int"),
        col("locations_1_teamSize").alias("LocationTeamSize1"),
        col("locations_1_address_formatted_address").alias("LocationAddress1"),
        col("locations_1_address_location_type").alias("LocationType1"),
        col("locations_1_address_latitude").alias("Latitude1"),
        col("locations_1_address_longitude").alias("Longitude1"),
        col("locations_2_comment").alias("LocationComment2"),
        col("locations_2_founded").alias("LocationFounded2").cast("int"),
        col("locations_2_teamSize").alias("LocationTeamSize2"),
        col("locations_2_address_formatted_address").alias("LocationAddress2"),
        col("locations_2_address_location_type").alias("LocationType2"),
        col("locations_2_address_latitude").alias("Latitude2"),
        col("locations_2_address_longitude").alias("Longitude2"),
        col("locations_3_comment").alias("LocationComment3"),
        col("locations_3_founded").alias("LocationFounded3").cast("int"),
        col("locations_3_teamSize").alias("LocationTeamSize3"),
        col("locations_3_address_formatted_address").alias("LocationAddress3"),
        col("locations_3_address_location_type").alias("LocationType3"),
        col("locations_3_address_latitude").alias("Latitude3"),
        col("locations_3_address_longitude").alias("Longitude3"),
        col("IngestionDate")
      )
)

# COMMAND ----------

# apply getAddress_udf function
df_locations_noblehire_enriched = (
    df_locations_noblehire_enriched
    .withColumn("LocationAddress0", when(col("LocationType0").isin(["ROOFTOP", "RANGE_INTERPOLATED", "GEOMETRIC_CENTER"]), getAddress_udf("LocationAddress0")).otherwise(col("LocationAddress0")))
    .withColumn("LocationAddress1", when(col("LocationType1").isin(["ROOFTOP", "RANGE_INTERPOLATED", "GEOMETRIC_CENTER"]), getAddress_udf("LocationAddress1")).otherwise(col("LocationAddress1")))
    .withColumn("LocationAddress2", when(col("LocationType2").isin(["ROOFTOP", "RANGE_INTERPOLATED", "GEOMETRIC_CENTER"]), getAddress_udf("LocationAddress2")).otherwise(col("LocationAddress2"))) 
    .withColumn("LocationAddress3", when(col("LocationType3").isin(["ROOFTOP", "RANGE_INTERPOLATED", "GEOMETRIC_CENTER"]), getAddress_udf("LocationAddress3")).otherwise(col("LocationAddress3")))
    .drop("LocationType0", "LocationType1", "LocationType2", "LocationType3")
)

# apply teamSizeBuckets function
df_locations_noblehire_enriched = (
    df_locations_noblehire_enriched
    .withColumn("LocationTeamSize0", teamSizeBuckets_udf("LocationTeamSize0"))
    .withColumn("LocationTeamSize1", teamSizeBuckets_udf("LocationTeamSize1"))
    .withColumn("LocationTeamSize2", teamSizeBuckets_udf("LocationTeamSize2"))
    .withColumn("LocationTeamSize3", teamSizeBuckets_udf("LocationTeamSize3"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 Setup

# COMMAND ----------

# MAGIC %md
# MAGIC The following commands are used for the initial setup of the delta table

# COMMAND ----------

# DBTITLE 1,Add SCD Type 2 Columns to Delta Table
df_locations_noblehire_enriched = (
    df_locations_noblehire_enriched
    .withColumn("IsActive", lit(True))
    .withColumn("StartDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("EndDate", lit(None).cast(StringType()))
)

# COMMAND ----------

# DBTITLE 1,Populate Delta Table, if empty
df_locations_noblehire_enriched.createOrReplaceTempView("Temp_DimLocations")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO WAREHOUSE.DimLocations (  
# MAGIC   LocationId,
# MAGIC   CompanyId,
# MAGIC   SourceSystem,
# MAGIC   LocationComment0,
# MAGIC   LocationFounded0,
# MAGIC   LocationTeamSize0,
# MAGIC   LocationAddress0,
# MAGIC   Latitude0,
# MAGIC   Longitude0,
# MAGIC   LocationComment1,
# MAGIC   LocationFounded1,
# MAGIC   LocationTeamSize1,
# MAGIC   LocationAddress1,
# MAGIC   Latitude1,
# MAGIC   Longitude1,
# MAGIC   LocationComment2,
# MAGIC   LocationFounded2,
# MAGIC   LocationTeamSize2,
# MAGIC   LocationAddress2,
# MAGIC   Latitude2,
# MAGIC   Longitude2,
# MAGIC   LocationComment3,
# MAGIC   LocationFounded3,
# MAGIC   LocationTeamSize3,
# MAGIC   LocationAddress3,
# MAGIC   Latitude3,
# MAGIC   Longitude3,
# MAGIC   IngestionDate,
# MAGIC   IsActive,           
# MAGIC   StartDate,          
# MAGIC   EndDate            
# MAGIC )
# MAGIC SELECT * FROM Temp_DimLocations

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 Logic

# COMMAND ----------

# DBTITLE 1,Create Temp Staging Table
# Create the Source Data Frame
sourceDF = df_locations_noblehire_enriched
sourceDF.display()
print("Count: {}".format(sourceDF.count()))

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
deltaLocations = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimLocations")

targetDF = deltaLocations.toDF()
targetDF.display()

# COMMAND ----------

# DBTITLE 1,Join source and target
targetDF = targetDF.filter(col("IsActive") == True).select(*[c for c in targetDF.columns if c not in ["IsActive", "StartDate", "EndDate"]])


joinDF = (
    sourceDF
    .join(
        targetDF, 
        (sourceDF.LocationId == targetDF.LocationId),
#         & (targetDF.IsActive == "true"),
        "outer"
    )
    .select(
        sourceDF["*"],
        targetDF.LocationId.alias("target_LocationId"),
        targetDF.CompanyId.alias("target_CompanyId"),
        targetDF.SourceSystem.alias("target_SourceSystem"),
        targetDF.LocationComment0.alias("target_LocationComment0"),
        targetDF.LocationFounded0.alias("target_LocationFounded0"),
        targetDF.LocationTeamSize0.alias("target_LocationTeamSize0"),
        targetDF.LocationAddress0.alias("target_LocationAddress0"),
        targetDF.Latitude0.alias("target_Latitude0"),
        targetDF.Longitude0.alias("target_Longitude0"),
        targetDF.LocationComment1.alias("target_LocationComment1"),
        targetDF.LocationFounded1.alias("target_LocationFounded1"),
        targetDF.LocationTeamSize1.alias("target_LocationTeamSize1"),
        targetDF.LocationAddress1.alias("target_LocationAddress1"),
        targetDF.Latitude1.alias("target_Latitude1"),
        targetDF.Longitude1.alias("target_Longitude1"),
        targetDF.LocationComment2.alias("target_LocationComment2"),
        targetDF.LocationFounded2.alias("target_LocationFounded2"),
        targetDF.LocationTeamSize2.alias("target_LocationTeamSize2"),
        targetDF.LocationAddress2.alias("target_LocationAddress2"),
        targetDF.Latitude2.alias("target_Latitude2"),
        targetDF.Longitude2.alias("target_Longitude2"),
        targetDF.LocationComment3.alias("target_LocationComment3"),
        targetDF.LocationFounded3.alias("target_LocationFounded3"),
        targetDF.LocationTeamSize3.alias("target_LocationTeamSize3"),
        targetDF.LocationAddress3.alias("target_LocationAddress3"),
        targetDF.Latitude3.alias("target_Latitude3"),
        targetDF.Longitude3.alias("target_Longitude3"),
        targetDF.IngestionDate.alias("target_IngestionDate")
    )
)

joinDF.display()

# COMMAND ----------

# DBTITLE 1,Hash source and target columns and compare them
filterDF = joinDF.filter(xxhash64(*[c for c in joinDF.columns if c.startswith("target") == False and "IngestionDate" not in c]) != xxhash64(*[c for c in joinDF.columns if c.startswith("target") == True and "IngestionDate" not in c])).withColumn("MergeKey", col("target_LocationId"))

filterDF.display()

# COMMAND ----------

# DBTITLE 1,Add MergeKey and set it to null where Id is not null
dummyDF = filterDF.filter(col("target_LocationId").isNotNull()).withColumn("MergeKey", lit(None))

dummyDF.display()

# COMMAND ----------

# DBTITLE 1,Union DFs
scdDF = filterDF.union(dummyDF)

scdDF.display()

# COMMAND ----------

# DBTITLE 1,Merge
(deltaLocations.alias("target")
 .merge(
     scdDF.alias("source"),
     "target.LocationId = source.MergeKey"
 )
 .whenMatchedUpdate(set = 
    {
        "SourceSystem": "'Noblehire.io'",
        "IsActive": "'False'", 
        "EndDate": "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')"
    }
 )
 .whenNotMatchedInsert(
     condition = "source.LocationId IS NOT NULL",
     values =
     {
        "LocationId": "source.LocationId",
        "CompanyId": "source.CompanyId",
        "SourceSystem": "source.SourceSystem",
        "LocationComment0": "source.LocationComment0",
        "LocationFounded0": "source.LocationFounded0",
        "LocationTeamSize0": "source.LocationTeamSize0",
        "LocationAddress0": "source.LocationAddress0",
        "Latitude0": "source.Latitude0",
        "Longitude0": "source.Longitude0",
        "LocationComment1": "source.LocationComment1",
        "LocationFounded1": "source.LocationFounded1",
        "LocationTeamSize1": "source.LocationTeamSize1",
        "LocationAddress1": "source.LocationAddress1",
        "Latitude1": "source.Latitude1",
        "Longitude1": "source.Longitude1",
        "LocationComment2": "source.LocationComment2",
        "LocationFounded2": "source.LocationFounded2",
        "LocationTeamSize2": "source.LocationTeamSize2",
        "LocationAddress2": "source.LocationAddress2",
        "Latitude2": "source.Latitude2",
        "Longitude2": "source.Longitude2",
        "LocationComment3": "source.LocationComment3",
        "LocationFounded3": "source.LocationFounded3",
        "LocationTeamSize3": "source.LocationTeamSize3",
        "LocationAddress3": "source.LocationAddress3",
        "Latitude3": "source.Latitude3",
        "Longitude3": "source.Longitude3",
        "IngestionDate": "source.IngestionDate",
        "IsActive": "'True'",
        "StartDate": "current_timestamp",
        "EndDate": """to_date('9999-12-31 00:00:00.0000', 'MM-dd-yyyy HH:mm:ss')"""
     }
 )
 .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# DBTITLE 1,Check Delta Table History
deltaLocations.history().display()

# COMMAND ----------

# DBTITLE 1,Compare Delta Table records with records in the Source DataFrame
# Read delta table into DataFrame
deltaFinal = DeltaTable.forPath(spark, "/mnt/adlslirkov/it-job-boards/Warehouse/DimLocations")
finalTargetDF = deltaFinal.toDF()

# Raise error if there are records in the delta table (when filtered to show only active records), which do not exists in the source DataFrame
targetExceptSourceCount = finalTargetDF.where(col("IsActive") == True).select("LocationId").exceptAll(sourceDF.select("LocationId")).count()
targetEqualsSourceCount = finalTargetDF.where(col("IsActive") == True).count() == sourceDF.count()

if targetExceptSourceCount > 0 or targetEqualsSourceCount == False:
    raise Exception("There are records in source, which do not exist in target.")
