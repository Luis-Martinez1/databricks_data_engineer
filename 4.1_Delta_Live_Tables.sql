-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Delta Live Tables

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Layer Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### orders_raw

-- COMMAND ----------

-- This SQL code creates a streaming Live Table called "orders_raw". The table is populated by reading and ingesting data from JSON files located in the specified path. The code uses the "cloud_files" function to read the files and the "map" function to specify additional options, such as inferring column types from the data.

CREATE OR REFRESH STREAMING LIVE TABLE orders_raw
COMMENT "The raw books orders, ingested from orders-raw"
AS SELECT * FROM cloud_files("${datasets_path}/orders-json-raw", "json",
                             map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- This SQL statement is for creating or refreshing a streaming live table named orders_raw in a data management system that supports SQL syntax for streaming and live data processing, such as Databricks SQL. Here's a breakdown of what each part of the statement does:

-- CREATE OR REFRESH STREAMING LIVE TABLE orders_raw: This command is used to either create a new streaming live table named orders_raw if it doesn't already exist, or update (refresh) it if it does. Streaming live tables are designed to handle real-time data streams, allowing for continuous updates and queries.

-- COMMENT "The raw books orders, ingested from orders-raw": This part adds a comment to the table definition, describing the purpose or content of the table. In this case, the comment indicates that the table contains raw book orders ingested from an unspecified source named orders-raw.

-- AS SELECT * FROM cloud_files(...): The AS SELECT * part specifies that the content of the orders_raw table will be populated by selecting all columns from the result of the cloud_files(...) function.

-- cloud_files("${datasets_path}/orders-json-raw", "json", map("cloudFiles.inferColumnTypes", "true")): This function call is responsible for ingesting data into the table. It specifies that data will be loaded from files located at a path indicated by ${datasets_path}/orders-json-raw. The path likely includes a variable component, ${datasets_path}, which would be replaced with an actual path at runtime. The files are in JSON format, as indicated by the "json" argument. The function also specifies an option (map("cloudFiles.inferColumnTypes", "true")) instructing the system to automatically infer the data types of the columns in the JSON files, which helps in correctly parsing and storing the data in the table.

-- In summary, this SQL code is setting up a live data streaming table to continuously ingest and update its content based on JSON files located at a specified path, with automatic data type inference for the columns in the ingested files.

CREATE OR REFRESH STREAMING LIVE TABLE orders_raw
COMMENT "The raw books orders, ingested from orders-raw"
AS SELECT * FROM cloud_files("${datasets_path}/orders-json-raw","json", map("cloudFiles.inferColumnTypes", "true") )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### customers

-- COMMAND ----------

-- The SQL code you've provided is for creating or refreshing a "live" table within a database system that supports such functionality, with this example likely tailored to a specific implementation like Databricks SQL. Here's a breakdown of the code:
-- CREATE OR REFRESH LIVE TABLE customers: This statement initiates the creation of a new live table named 'customers', or if the table already exists, it refreshes its data. A 'live' table is a type of table that is designed to automatically update or refresh with the latest data according to a defined schedule or trigger, ensuring that the most current data is always available.
-- COMMENT "The customers lookup table, ingested from customers-json": This part adds a comment to the table being created or refreshed. The comment serves as a documentation or note, explaining that this table is a "customers lookup table" and its data is sourced from 'customers-json'.
-- The AS SELECT * FROM json line of code means the JSON file's data is the source for the 'customers' table, and the entire file is read to populate the table.
-- To summarize, this SQL script is designed to create or refresh a live table named 'customers' with the data ingested from a JSON file located at a specific path. The table serves as a lookup for customer information, and its content is kept up-to-date automatically, assuming the system supports such live table functionality.


CREATE OR REFRESH LIVE TABLE customers
COMMENT "The customers lookup table, ingested from customers-json"
AS SELECT * FROM json.`${datasets_path}/customers-json`

-- COMMAND ----------

-- The given SQL code is using the "DESCRIBE" statement to view the structure and details of the "default" database. This code is likely used in a database management system to display the metadata of the database, such as the table names, column names, data types, and any constraints or indexes present.

DESCRIBE DATABASE default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Silver Layer Tables
-- MAGIC
-- MAGIC #### orders_cleaned

-- COMMAND ----------

-- This SQL code snippet is specific to a system that supports streaming and live tables, likely a modern data processing platform like Databricks. The code is for creating or refreshing a streaming live table named orders_cleaned from raw orders data. Here's a concise explanation:

-- Create or Refresh Streaming Live Table: The statement begins with CREATE OR REFRESH STREAMING LIVE TABLE orders_cleaned. This indicates the creation or updating of a live table named orders_cleaned, intended for holding cleaned data from raw orders. This kind of table is live and streaming, meaning it continuously updates as new data flows in.

-- Constraints: CONSTRAINT valid_order_number EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW is specified to ensure data quality. It sets a condition that order_id must not be NULL. Rows that violate this condition (i.e., have a NULL order_id) are automatically dropped, ensuring only valid data is included.

-- Comment: COMMENT "The cleaned books orders with valid order_id" provides a description of the table's purpose, which is to store cleaned records of book orders that have a valid order identifier.

-- Select Statement: The AS SELECT ... part defines how the data should be selected and transformed for insertion into the orders_cleaned table. It selects several columns from the orders_raw live table, performs a left join with the customers live table based on customer_id, and applies transformations:

-- order_id, quantity, o.customer_id: Selects these fields directly from the raw orders.
-- c.profile:first_name as f_name, c.profile:last_name as l_name: Extracts the first and last name from the nested profile field in the customers table and aliases them as f_name and l_name.
-- cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp: Converts the order_timestamp from a UNIX timestamp to a human-readable datetime format.
-- o.books: Selects the books from the orders.
-- c.profile:address:country as country: Extracts the country from the nested profile:address field in the customers table.
-- From STREAM and JOIN: The FROM STREAM(LIVE.orders_raw) o LEFT JOIN LIVE.customers c ON o.customer_id = c.customer_id part indicates streaming data from the orders_raw live table, joining it with the customers live table on customer_id. This join is left-based, meaning all rows from the orders_raw table will be included even if there's no matching row in the customers table.

-- In summary, this code is about creating a dynamically updating table that contains cleaned and structured data related to book orders, by transforming and combining data from the raw orders and customers tables.

CREATE OR REFRESH STREAMING LIVE TABLE orders_cleaned (
  CONSTRAINT valid_order_number EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned books orders with valid order_id"
AS
  SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
         cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, o.books,
         c.profile:address:country as country
  FROM STREAM(LIVE.orders_raw) o
  LEFT JOIN LIVE.customers c
    ON o.customer_id = c.customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC >> Constraint violation
-- MAGIC
-- MAGIC | **`ON VIOLATION`** | Behavior |
-- MAGIC | --- | --- |
-- MAGIC | **`DROP ROW`** | Discard records that violate constraints |
-- MAGIC | **`FAIL UPDATE`** | Violated constraint causes the pipeline to fail  |
-- MAGIC | Omitted | Records violating constraints will be kept, and reported in metrics |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Gold Tables

-- COMMAND ----------

-- This SQL code creates or refreshes a live table called "cn_daily_customer_books" that represents the daily number of books purchased by each customer in China.

-- The table is populated by selecting data from the "orders_cleaned" table, filtering for orders made in China.

-- The selected columns include the customer ID, first name, last name, order date (truncated to the day), and the sum of the "quantity" column which represents the number of books purchased in each order.

-- The data is then grouped by customer ID, first name, last name, and order date to calculate the total number of books purchased by each customer on each day.


CREATE OR REFRESH LIVE TABLE cn_daily_customer_books
COMMENT "Daily number of books per customer in China"
AS
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM LIVE.orders_cleaned
  WHERE country = "China"
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)

-- COMMAND ----------

-- This SQL code represents a command designed to create or refresh a live table named fr_daily_customer_books, which is intended to store data regarding the daily number of books ordered by customers in France. Here's a breakdown of the key components of the command:

-- CREATE OR REFRESH LIVE TABLE: This command is aimed at either creating a new live table if it doesn't exist or refreshing the data if the table already exists. A live table is a concept that usually refers to a dynamically updating table, meaning its content can change as the underlying data changes.

-- fr_daily_customer_books: This is the name given to the live table. It implies the table will contain data about French customers' daily book orders.

-- COMMENT: The text "Daily number of books per customer in France" is a description added to the table for better understanding and maintainability.

-- AS: This keyword introduces the SELECT statement that defines the structure and data that will populate the live table.

-- SELECT statement: It specifies the columns to be included in the live table:

-- customer_id, f_name, and l_name identify the customer.
-- date_trunc("DD", order_timestamp) as order_date was used to truncate the order_timestamp to the day level, ensuring that all orders within the same day are grouped together, regardless of the time they were made. It's renamed as order_date.
-- sum(quantity) as books_counts calculates the total number of books ordered by each customer on each day.
-- FROM LIVE.orders_cleaned: Indicates that the data is being selected from a live or dynamically updated table named orders_cleaned.

-- WHERE country = "France": This condition filters the data to only include orders made in France.

-- GROUP BY: This clause groups the selected rows by customer_id, f_name, l_name, and the date truncated order timestamp. It's essential for the aggregation function sum(quantity) to work correctly, as it calculates the total books ordered for each unique combination of these grouping columns.

-- In summary, this SQL command is intended to maintain a live and updated daily record of the number of books ordered by each customer in France, capturing the essential details like who ordered, when, and how much.


CREATE OR REFRESH LIVE TABLE fr_daily_customer_books
COMMENT "Daily number of books per customer in France"
AS
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM LIVE.orders_cleaned
  WHERE country = "France"
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)

-- COMMAND ----------


