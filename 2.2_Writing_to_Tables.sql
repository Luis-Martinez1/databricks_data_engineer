-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

--The code is invoking the script Copy-Datasets located in the ../Includes directory. This script is responsible for copying datasets from one location to another.
%run ../Includes/Copy-Datasets

-- COMMAND ----------

--The SQL code provided is creating a new table called "orders_lm" by selecting all data from an existing table called "orders" in the dataset specified. The data is loaded from a parquet file format.
CREATE TABLE orders_lm AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

SELECT * FROM orders_lm

-- COMMAND ----------

-- This SQL code creates a new table called "orders_lm" by retrieving all the data from the "orders" table in the "bookstore" dataset. The data is accessed from a Parquet file format. Any existing "orders_lm" table will be replaced.
CREATE OR REPLACE TABLE orders_lm AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders_lm

-- COMMAND ----------

--This SQL code is inserting data into a table called "orders_lm" using the "INSERT OVERWRITE" statement. The data being inserted is selected from a Parquet file located at the path specified by the variable "${dataset.bookstore}/orders".
INSERT OVERWRITE orders_lm
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders_lm

-- COMMAND ----------

-- The code is performing an INSERT OVERWRITE operation on a table called "orders_lm". It is selecting all columns from a Parquet file located in the ${dataset.bookstore}/orders directory. Additionally, it is adding a new column called "timestamp_col" to the selected data, which will contain the current timestamp at the time of the INSERT operation.
INSERT OVERWRITE orders_lm
SELECT *, current_timestamp() as timestamp_col FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

--The code is inserting data into a table named "orders_lm" by selecting all columns from a parquet file called "orders" stored in a dataset named "bookstore". It also adds a new column named "timestamp_col" with the current timestamp. The "OPTIONS ('mergeSchema'='true')" is used to merge the schema of the parquet file with the existing schema of the "orders_lm" table if they differ.

INSERT OVERWRITE orders_lm
SELECT *, current_timestamp() as timestamp_col 
FROM parquet.`${dataset.bookstore}/orders`
OPTIONS ('mergeSchema'='true')

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM parquet.`${dataset.bookstore}/orders-new`

-- COMMAND ----------

SELECT count(*) FROM orders

-- COMMAND ----------

--This SQL code creates a temporary view called "customers_updates" by fetching data from a JSON file in a dataset named "bookstore". The JSON data represents updates to the "customers" table.

--Then, a MERGE statement is used to merge the data from the "customers_updates" view with the "customers" table. The merging is based on matching records using the "customer_id" column.

--The MERGE statement has two conditions:

--If a matching record is found, and the email in the "customers" table is NULL but the email in the "customers_updates" view is NOT NULL, then the email and updated date in the "customers" table are updated with the values from the "customers_updates" view.
--If no matching record is found, then the entire record from the "customers_updates" view is inserted into the "customers" table.
--In summary, this code applies updates to the "customers" table using the data from the "customers_updates" view and inserts new records if no match is found.

CREATE OR REPLACE TEMP VIEW customers_updates AS 
SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;

MERGE INTO customers c
USING customers_updates u
ON c.customer_id = u.customer_id
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
  UPDATE SET email = u.email, updated = u.updated
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

--The given SQL code creates a temporary view called "books_updates" based on a CSV file. The view has five columns: book_id, title, author, category, and price. It specifies that the CSV file is located in the specified path and has a header row and a delimiter of ";".

--It then executes a SELECT statement to retrieve all rows from the "books_updates" view.

CREATE OR REPLACE TEMP VIEW books_updates
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv-new",
  header = "true",
  delimiter = ";"
);

SELECT * FROM books_updates

-- COMMAND ----------

--The code is performing a merge operation on a table called "books" and a table called "books_updates". It is merging the data from "books_updates" into "books" based on matching values in the "book_id" and "title" columns.

--If a match is found, meaning the same book is already in the "books" table, the code will update the existing record with the values from "books_updates".

--If no match is found, and the category in "books_updates" is 'Computer Science', then a new record will be inserted into the "books" table with the values from "books_updates".

--The merge operation essentially combines the update and insert operations into one statement based on certain conditions.

MERGE INTO books b
USING books_updates u
ON b.book_id = u.book_id AND b.title = u.title
WHEN NOT MATCHED AND u.category = 'Computer Science' THEN 
  INSERT *
