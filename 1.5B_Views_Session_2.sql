-- Databricks notebook source
--The code is a SQL statement that will return the names of all tables in the current database. It is commonly used to retrieve a list of tables in a database when performing database management tasks.

SHOW TABLES;

-- COMMAND ----------

--This code is used to show the tables present in the global_temp database.
SHOW TABLES IN global_temp;

-- COMMAND ----------

--The given SQL code is selecting all columns from the table named "global_temp_view_latest_phones" in the global temporary view called "global_temp".
SELECT * FROM global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

-- This code is used to drop a table named "smartphones" and two views named "view_apple_phones" and "global_temp.global_temp_view_latest_phones". Dropping a table or a view means deleting it permanently from the database.
DROP TABLE smartphones;

DROP VIEW view_apple_phones;
DROP VIEW global_temp.global_temp_view_latest_phones;

-- COMMAND ----------


