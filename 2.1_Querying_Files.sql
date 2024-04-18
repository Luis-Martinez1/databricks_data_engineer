-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

--The code is including the "Copy-Datasets" module from the "../Includes" directory. This module likely contains functions or code that copies datasets from one location to another. By including this module, the code can utilize those functions or code to perform dataset copying operations.
%run ../Includes/Copy-Datasets

-- COMMAND ----------

-- This code retrieves a list of files in a directory stored in the dataset_bookstore variable. The ls function is used to list the files and display is used to show the list in a nice format.
%python
files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
display(files)

-- COMMAND ----------

--The SQL code is querying data from a JSON file. It uses the "json" function to access the JSON data and the "SELECT" statement to retrieve all columns and rows from the specified JSON file. The ${dataset.bookstore} is a placeholder for the dataset name and the export_001.json is the specific JSON file being queried.
SELECT * FROM json.`${dataset.bookstore}/customers-json/export_001.json`

-- COMMAND ----------

--The code is using the SELECT statement to retrieve all data from multiple JSON files located under the specified dataset and directory. The location is specified using the ${dataset.bookstore} placeholder, which needs to be replaced with the actual dataset name. The export_*.json pattern is used to match all files with names starting with "export_" and ending with ".json".
SELECT * FROM json.`${dataset.bookstore}/customers-json/export_*.json`

-- COMMAND ----------

--The code is querying data from a JSON dataset named ${dataset.bookstore}. It selects all columns and rows from the customers-json table.

SELECT * FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT count(*) FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

 --This SQL code is querying a JSON dataset called customers-json within a dataset called bookstore. The query selects all columns from the dataset and adds a new column called source_file which contains the name of the file from which the data is being retrieved.
 
 SELECT *,
    input_file_name() source_file
  FROM json.`${dataset.bookstore}/customers-json`;

-- COMMAND ----------

SELECT * FROM text.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT * FROM binaryFile.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT * FROM csv.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

--The code creates a table named "books_csv_lm" in a SQL database. The table has five columns: book_id, title, author, category, and price. It uses the CSV file format as the data source and specifies that the CSV file contains a header row and the delimiter between values is a semicolon (;). The location of the CSV file is specified using a variable called "${dataset.bookstore}/books-csv".

CREATE TABLE books_csv_lm
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  header = "true",
  delimiter = ";"
)
LOCATION "${dataset.bookstore}/books-csv"

-- COMMAND ----------

SELECT * FROM books_csv_lm

-- COMMAND ----------

DESCRIBE EXTENDED books_csv_lm

-- COMMAND ----------

--The code is using the dbutils.fs.ls() function to list the files present in the "books-csv" directory in the "dataset_bookstore" folder. It then displays the list of files.
%python
files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
display(files)

-- COMMAND ----------

--This code reads a table named "books_csv" and writes it to a CSV file located in a dataset called "dataset_bookstore". The mode is set to "append", which means that if the file already exists, new data will be appended to it. The format of the output file is CSV, and the options "header" and "delimiter" are set to "true" and ";" respectively.
%python
(spark.read
        .table("books_csv")
      .write
        .mode("append")
        .format("csv")
        .option('header', 'true')
        .option('delimiter', ';')
        .save(f"{dataset_bookstore}/books-csv"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

REFRESH TABLE books_csv

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

CREATE TABLE customers_lm AS
SELECT * FROM json.`${dataset.bookstore}/customers-json`;

DESCRIBE EXTENDED customers;

-- COMMAND ----------

--The code creates a new table called "books_unparsed_lm" by copying all the data from a CSV file named "books-csv" in the specified dataset. Then, it retrieves all the data from the "books_unparsed" table and displays it
CREATE TABLE books_unparsed_lm AS
SELECT * FROM csv.`${dataset.bookstore}/books-csv`;

SELECT * FROM books_unparsed;

-- COMMAND ----------

-- This code creates a temporary view called "books_tmp_lm2" by loading data from CSV files in a specific path. It assumes the CSV files have headers and use a semicolon delimeter.

--Afterwards, it creates a permanent table called "books_lm" by copying the data from the temporary view.

--Finally, it selects all records from the "books_lm" table and returns the result.

CREATE TEMP VIEW books_tmp_lm2
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv/export_*.csv",
  header = "true",
  delimiter = ";"
);

CREATE TABLE books_lm AS
  SELECT * FROM books_tmp_lm2;
  
SELECT * FROM books_lm

-- COMMAND ----------

DESCRIBE EXTENDED books_lm

-- COMMAND ----------

'''The SQL code provided retrieves current timestamp using the function current_timestamp().

It then extracts various components from the timestamp using different functions:

date(current_timestamp()) extracts the date component from the timestamp and aliases it as _date.
year(current_timestamp()) extracts the year component from the timestamp and aliases it as _year.
month(current_timestamp()) extracts the month component from the timestamp and aliases it as _month.
day(current_timestamp()) extracts the day component from the timestamp and aliases it as _day.
hour(current_timestamp()) extracts the hour component from the timestamp and aliases it as _hour.
minute(current_timestamp()) extracts the minute component from the timestamp and aliases it as _minute.
These aliases are used to represent the extracted components for further analysis or representation.
'''

select current_timestamp()
     , date(current_timestamp())  as _date    
     , year(current_timestamp())  as _year
     , month(current_timestamp()) as _month
     , day(current_timestamp())   as _day
     , hour(current_timestamp())  as _hour
     , minute(current_timestamp())  as _minute


-- COMMAND ----------


