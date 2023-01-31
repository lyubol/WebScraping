# Databricks notebook source
from pyspark.sql.functions import explode, sequence, to_date

beginDate = '2020-01-01'
endDate = '2050-12-31'

(
  spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as calendarDate")
    .createOrReplaceTempView('dates')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE WAREHOUSE.DimDate
# MAGIC USING delta
# MAGIC LOCATION '/mnt/adlslirkov/it-job-boards/Warehouse/DimDate'
# MAGIC AS SELECT 
# MAGIC   YEAR(calendarDate) * 10000 + MONTH(calendarDate) * 100 + DAY(calendarDate) AS DateKey, 
# MAGIC   CalendarDate, 
# MAGIC   YEAR(calendarDate) AS CalendarYear, 
# MAGIC   DATE_FORMAT(calendarDate, 'MMMM') AS CalendarMonth, 
# MAGIC   MONTH(calendarDate) AS MonthOfYear, 
# MAGIC   DATE_FORMAT(calendarDate, 'EEEE') AS CalendarDay, 
# MAGIC   DAYOFWEEK(calendarDate) AS DayOfWeek, 
# MAGIC   WEEKDAY(calendarDate) + 1 AS DayOfWeekStartMonday, 
# MAGIC   CASE 
# MAGIC     WHEN WEEKDAY(calendarDate) < 5 THEN 'Y'
# MAGIC     ELSE 'N'
# MAGIC   END AS IsWeekDay, 
# MAGIC   DAYOFMONTH(calendarDate) AS DayOfMonth, 
# MAGIC   CASE 
# MAGIC     WHEN calendarDate = LAST_DAY(calendarDate) THEN 'Y'
# MAGIC     ELSE 'N'
# MAGIC   END AS IsLastDayOfMonth, 
# MAGIC   DAYOFYEAR(calendarDate) AS DayOfYear, 
# MAGIC   WEEKOFYEAR(calendarDate) AS WeekOfYearIso, 
# MAGIC   QUARTER(calendarDate) AS QuarterOfYear
# MAGIC FROM dates;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM WAREHOUSE.DimDate
