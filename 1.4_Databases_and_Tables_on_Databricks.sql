-- Databricks notebook source
-- This code is creating a new table called "managed_default_lm_new" with three columns: "width", "length", and "height". The data type for all three columns is integer (INT).

--Then, the code is inserting a single row into the "managed_default_lm_new" table with values 3 for width, 2 for length, and 1 for height.

CREATE TABLE managed_default_lm_new
  (width INT, length INT, height INT);

INSERT INTO managed_default_lm_new
VALUES (3 , 2 , 1 )

-- COMMAND ----------

DESCRIBE EXTENDED managed_default_lm_new

-- COMMAND ----------

CREATE TABLE external_default_lm_new2
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_default_lm_new2';
  
INSERT INTO external_default_lm_new2
VALUES (3 , 2 , 1 )

-- COMMAND ----------

--The SQL code is using the "DESCRIBE EXTENDED" statement to get the extended description of the table named "external_default_lm_new2". The output will provide detailed information about the columns, data types, and other properties of the table.
DESCRIBE EXTENDED external_default_lm_new2

-- COMMAND ----------

DROP TABLE managed_default_lm_new

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/managed_default'

-- COMMAND ----------

DROP TABLE external_default

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_default'

-- COMMAND ----------

CREATE SCHEMA new_default

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED new_default

-- COMMAND ----------

-- The code begins by selecting a database named "new_default" to work with.

--It then creates a table named "managed_new_default" with three columns: width, length, and height, each of type integer.

--Next, it inserts a row with values 3, 2, and 1 into the "managed_new_default" table.

--After that, it creates another table named "external_new_default" with the same three columns. However, this table is created as an external table with its data located at 'dbfs:/mnt/demo/external_new_default'.

--Finally, it inserts a row with values 3, 2, and 1 into the "external_new_default" table.

USE new_default;

CREATE TABLE managed_new_default
  (width INT, length INT, height INT);
  
INSERT INTO managed_new_default
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------

CREATE TABLE external_new_default
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_new_default';
  
INSERT INTO external_new_default
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED managed_new_default

-- COMMAND ----------

DESCRIBE EXTENDED external_new_default

-- COMMAND ----------

DROP TABLE managed_new_default;
DROP TABLE external_new_default;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/managed_new_default'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_new_default'

-- COMMAND ----------

CREATE SCHEMA custom
LOCATION 'dbfs:/Shared/schemas/custom.db'

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED custom

-- COMMAND ----------

--The SQL code creates two tables: "managed_custom" and "external_custom".

--The "managed_custom" table has three columns: width (INT), length (INT), and height (INT). It is created within the current database.

--The "external_custom" table also has three columns: width (INT), length (INT), and height (INT). However, it is created as an external table and its data is stored in the location 'dbfs:/mnt/demo/external_custom', which is a file system path.

--Additionally, the code inserts a single row into each table. The values inserted are 3 for width, 2 for length, and 1 for height in both cases.


USE custom;

CREATE TABLE managed_custom
  (width INT, length INT, height INT);
  
INSERT INTO managed_custom
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------

CREATE TABLE external_custom
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_custom';
  
INSERT INTO external_custom
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED managed_custom

-- COMMAND ----------

DESCRIBE EXTENDED external_custom

-- COMMAND ----------

DROP TABLE managed_custom;
DROP TABLE external_custom;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/Shared/schemas/custom.db/managed_custom'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_custom'

-- COMMAND ----------


