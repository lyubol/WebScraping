-- Databricks notebook source
-- DBTITLE 1,Create database `jobposts` only if database with same name doesn't exist
CREATE DATABASE IF NOT EXISTS jobposts;

USE jobposts;

-- COMMAND ----------

-- DBTITLE 1,-- Create table `posts` only if table with same name doesn't exist.
CREATE TABLE IF NOT EXISTS jobposts.posts(
  	Id STRING,
  	IngestionDate DATE,
  	Source STRING,
	Title STRING,
	Company STRING,
	Location STRING,
	Uploaded STRING,
	Salary STRING,
	Department STRING,
	Link STRING,
    	IsActive BOOLEAN
)

-- COMMAND ----------

SHOW TABLES
