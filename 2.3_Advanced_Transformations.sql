-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

SELECT * FROM customers

-- COMMAND ----------

DESCRIBE customers

-- COMMAND ----------

--This SQL code selects the customer ID, first name, and country from the "customers" table. It retrieves the first name from the "profile" field using the "profile:first_name" syntax and the country from the nested "address" field using the "profile:address:country" syntax.
SELECT customer_id, profile:first_name, profile:address:country 
FROM customers

-- COMMAND ----------

--The code selects the "profile" column from a table called "customers" and converts each value from JSON format to a more structured format. The result is a column named "profile_struct" that contains the profile data in a structured format instead of JSON.

SELECT from_json(profile) AS profile_struct
  FROM customers;

-- COMMAND ----------

SELECT profile 
FROM customers 
LIMIT 1

-- COMMAND ----------

--The code creates a temporary view called "parsed_customers" which extracts and parses the "profile" column in the "customers" table. It uses the schema_of_json function to define the structure of the JSON data. The parsed data is selected and displayed.

CREATE OR REPLACE TEMP VIEW parsed_customers AS
  SELECT customer_id, from_json(profile, schema_of_json('{"first_name":"Thomas","last_name":"Lane","gender":"Male","address":{"street":"06 Boulevard Victor Hugo","city":"Paris","country":"France"}}')) AS profile_struct
  FROM customers;
  
SELECT * FROM parsed_customers

-- COMMAND ----------

DESCRIBE parsed_customers

-- COMMAND ----------

--The SQL code retrieves the customer ID, first name, and country from the "parsed_customers" table. The "parsed_customers" table likely contains structured customer data, and the code uses the "." operator to access the "first_name" and "address.country" fields of the "profile_struct" column within the "parsed_customers" table. The code is selecting only the specified fields from the table.

SELECT customer_id, profile_struct.first_name, profile_struct.address.country
FROM parsed_customers

-- COMMAND ----------

--This SQL code creates a temporary view named "customers_final" that combines the "customer_id" column from the "parsed_customers" table with all columns from the "profile_struct" column in the same table. Then, it selects all rows from the "customers_final" view.

CREATE OR REPLACE TEMP VIEW customers_final AS
  SELECT customer_id, profile_struct.*
  FROM parsed_customers;
  
SELECT * FROM customers_final

-- COMMAND ----------

SELECT order_id, customer_id, books
FROM orders

-- COMMAND ----------

SELECT order_id, customer_id, explode(books) AS book 
FROM orders

-- COMMAND ----------

--The SQL code above is querying a table named "orders" and selecting the customer ID, along with two sets of data: the order IDs and the book IDs associated with each customer.

--The code uses the GROUP BY clause to group the data by customer ID.

--The function collect_set() is used to generate a set of unique values for each customer. In this case, it is used to create a set of unique order IDs and a set of unique book IDs for each customer.

--The result of the code will be a set of rows, where each row represents a customer and includes their customer ID, a set of order IDs they placed, and a set of book IDs associated with their orders.

SELECT customer_id,
  collect_set(order_id) AS orders_set,
  collect_set(books.book_id) AS books_set
FROM orders
GROUP BY customer_id

-- COMMAND ----------

--The SQL code selects the customer_id column from the orders table. It then applies the collect_set function to the book_id column, which collects all unique book IDs for each customer_id into an array called before_flatten.

--Next, it uses the flatten function to convert the array into a single column, effectively flattening the array. It then applies the array_distinct function to remove any duplicate values from the flattened array, creating a new column called after_flatten.

--Finally, the code groups the results by customer_id. In summary, the code collects all unique book IDs for each customer, flattens the array of book IDs, removes any duplicates, and groups the results by customer ID.

SELECT customer_id,
  collect_set(books.book_id) As before_flatten,
  array_distinct(flatten(collect_set(books.book_id))) AS after_flatten
FROM orders
GROUP BY customer_id

-- COMMAND ----------

--The code is creating a view called "orders_enriched" that joins the "orders" table with the "books" table. The view includes all columns from the "orders" table and also includes a new column called "book" that is created by exploding the "books" column in the "orders" table. The exploding operation splits the "books" array into separate rows for each element in the array. The view then performs an inner join with the "books" table based on the "book_id" column in both tables. Finally, the code selects all rows from the "orders_enriched" view to display the result.


CREATE OR REPLACE VIEW orders_enriched AS
SELECT *
FROM (
  SELECT *, explode(books) AS book 
  FROM orders) o
INNER JOIN books b
ON o.book.book_id = b.book_id;

SELECT * FROM orders_enriched

-- COMMAND ----------

--The code creates a temporary view named "orders_updates" by loading data from a parquet file located in the specified dataset.

--Then, it combines the data from the "orders" table with the data from the "orders_updates" view using the UNION operator. It returns all the rows from both sources without any duplicates

CREATE OR REPLACE TEMP VIEW orders_updates
AS SELECT * FROM parquet.`${dataset.bookstore}/orders-new`;

SELECT * FROM orders 
UNION 
SELECT * FROM orders_updates 

-- COMMAND ----------

--The SQL code retrieves all rows from the "orders" table that intersect with the rows from the "orders_updates" table. In other words, it will return only the rows that exist in both tables.

SELECT * FROM orders 
INTERSECT 
SELECT * FROM orders_updates 

-- COMMAND ----------

--This SQL code is querying data from two tables, "orders" and "orders_updates". It selects all data from the "orders" table and then subtracts any records that are also present in the "orders_updates" table. The result will be a list of records that exist in the "orders" table but do not exist in the "orders_updates" table.

SELECT * FROM orders 
MINUS 
SELECT * FROM orders_updates 

-- COMMAND ----------

--This code creates a new table called "transactions" by pivoting the data from the "orders_enriched" table. The pivot operation aggregates the quantity of books for each customer based on the book_id. The resulting table will have customer_id as the primary key and columns representing the sum of quantities for each book_id. The last statement then retrieves all the data from the "transactions" table.

CREATE OR REPLACE TABLE transactions AS

SELECT * FROM (
  SELECT
    customer_id,
    book.book_id AS book_id,
    book.quantity AS quantity
  FROM orders_enriched
) PIVOT (
  sum(quantity) FOR book_id in (
    'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
    'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
  )
);

SELECT * FROM transactions
