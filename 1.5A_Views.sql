-- Databricks notebook source
--The code creates a table called smartphones_lm if it does not already exist. The table has four columns: id, name, brand, and year.

--The code then inserts ten rows of data into the smartphones_lm table using the INSERT INTO statement. Each row represents a smartphone and includes values for the id, name, brand, and year columns.

--After running this code, the smartphones_lm table will exist and contain ten rows of smartphone data.

CREATE TABLE IF NOT EXISTS smartphones_lm
(id INT, name STRING, brand STRING, year INT);

INSERT INTO smartphones_lm
VALUES (1, 'iPhone 14', 'Apple', 2022),
      (2, 'iPhone 13', 'Apple', 2021),
      (3, 'iPhone 6', 'Apple', 2014),
      (4, 'iPad Air', 'Apple', 2013),
      (5, 'Galaxy S22', 'Samsung', 2022),
      (6, 'Galaxy Z Fold', 'Samsung', 2022),
      (7, 'Galaxy S9', 'Samsung', 2016),
      (8, '12 Pro', 'Xiaomi', 2022),
      (9, 'Redmi 11T Pro', 'Xiaomi', 2022),
      (10, 'Redmi Note 11', 'Xiaomi', 2021)

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

--The code creates a view called "view_apple_phones" by selecting all data from the "smartphones_lm" table where the brand is 'Apple'. This view will only display the rows where the brand is 'Apple' and can be used to retrieve this filtered data easily in future queries.

CREATE VIEW view_apple_phones
AS  SELECT * 
    FROM smartphones_lm
    WHERE brand = 'Apple';

-- COMMAND ----------

SELECT * FROM view_apple_phones;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

--This code creates a temporary view called "temp_view_phones_brands" that contains distinct values of the "brand" column from the "smartphones" table. It then selects all rows from the temporary view, displaying the distinct brands of smartphones.

CREATE TEMP VIEW temp_view_phones_brands
AS  SELECT DISTINCT brand
    FROM smartphones;

SELECT * FROM temp_view_phones_brands;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- This code creates a global temporary view named "global_temp_view_latest_phones". The view is created based on the "smartphones" table. It filters the data to only include smartphones with a year greater than 2020. The result is then ordered in descending order by year. The global temporary view can be accessed and used by any session but is automatically dropped at the end of the session.

CREATE GLOBAL TEMP VIEW global_temp_view_latest_phones
AS SELECT * FROM smartphones
    WHERE year > 2020
    ORDER BY year DESC;

-- COMMAND ----------

--The code selects all columns from the temporary view global_temp_view_latest_phones in the global temporary database.
SELECT * FROM global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------


