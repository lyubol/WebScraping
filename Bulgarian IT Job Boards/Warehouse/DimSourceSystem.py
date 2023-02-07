# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Define data and schema
source_systems_data = [(1, "Dev.bg"), (2, "Zaplata.bg"), (3, "Noblehire.io")]

schema = StructType(fields = [
    StructField("SourceSystemKey", IntegerType(), False),
    StructField("SourceSystemName", StringType(), False)
])

# COMMAND ----------

# DBTITLE 1,Create DimSourceSystems
df_dim_source_system = spark.createDataFrame(data = source_systems_data, schema = schema)

df_dim_source_system.write.format("delta").saveAsTable("WAREHOUSE.DimSourceSystems")
