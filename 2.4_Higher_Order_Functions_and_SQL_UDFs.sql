-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

--This SQL code is selecting the order_id and books columns from the "orders" table. It also includes a filter condition using the FILTER function.

--The FILTER function allows us to filter the "books" array column based on a specific condition. In this case, the condition is that only books with a quantity greater than or equal to 2 should be included in the "multiple_copies" column.

--So, the result of this SQL code will be a table with the order_id and books columns, along with a new column called "multiple_copies" which contains only the books that have a quantity of 2 or more.

SELECT
  order_id,
  books,
  FILTER (books, i -> i.quantity >= 2) AS multiple_copies
FROM orders

-- COMMAND ----------

--This SQL code retrieves the order ID and the books that have multiple copies from a table of orders. It uses a subquery to filter the books in each order and returns only those with a quantity of 2 or more. The outer query then selects the order IDs with at least one book having multiple copies, checking if the size of the multiple_copies array is greater than 0.

SELECT order_id, multiple_copies
FROM (
  SELECT
    order_id,
    FILTER (books, i -> i.quantity >= 2) AS multiple_copies
  FROM orders)
WHERE size(multiple_copies) > 0;

-- COMMAND ----------

--This code is performing a SELECT statement on the "orders" table. It will retrieve the order_id and books columns from the table.

--The interesting part is the TRANSFORM function which applies a transformation on the books column. For each value in the books column, it multiplies the subtotal by 0.8 and casts the result to an integer.

--The result of this transformation is stored in the column named "subtotal_after_discount".

--So, this code calculates the subtotal after applying an 80% discount to the subtotal for each book in the "books" column, and returns the order_id, original books, and discounted subtotal for each order.

SELECT
  order_id,
  books,
  TRANSFORM (
    books,
    b -> CAST(b.subtotal * 0.8 AS INT)
  ) AS subtotal_after_discount
FROM orders;

-- COMMAND ----------

--This SQL code creates a new function called "get_url" that takes a parameter "email" of type STRING.

--The function uses the "split" function to split the email string by the "@" symbol and returns the element at index 1 (the email provider domain).

--Finally, the function concatenates the domain with the prefix "https://www." and returns the resulting URL as a STRING value.

CREATE OR REPLACE FUNCTION get_url(email STRING)
RETURNS STRING

RETURN concat("https://www.", split(email, "@")[1])

-- COMMAND ----------

--This SQL code selects the "email" column from the "customers" table and uses the "get_url" function to extract the domain of each email. The result of the query includes two columns: "email" and "domain" which contains the extracted domain from each email.

SELECT email, get_url(email) domain
FROM customers

-- COMMAND ----------

--The SQL code DESCRIBE FUNCTION get_url is used to retrieve information about the function called "get_url". This code will provide details about the function, including its parameters, return type, and any other relevant information.

DESCRIBE FUNCTION get_url

-- COMMAND ----------

--The code is used to describe the details of a user-defined database function named "get_url" in SQL. The "DESCRIBE FUNCTION EXTENDED" statement provides information about the function, including its name, data types of the input parameters, return data type, and other relevant details.

DESCRIBE FUNCTION EXTENDED get_url

-- COMMAND ----------

--This SQL code defines a function called site_type that takes an email address as input. It then uses a CASE statement to determine the type of website associated with the email domain.

--If the email domain ends with ".com", the function returns "Commercial business". If it ends with ".org", the function returns "Non-profits organization". If it ends with ".edu", the function returns "Educational institution".

--If none of these conditions are met, the function uses the concat function to return a string that states the email domain is unknown, based on the domain extracted from the email address after the "@" symbol.

CREATE FUNCTION site_type(email STRING)
RETURNS STRING
RETURN CASE 
          WHEN email like "%.com" THEN "Commercial business"
          WHEN email like "%.org" THEN "Non-profits organization"
          WHEN email like "%.edu" THEN "Educational institution"
          ELSE concat("Unknow extenstion for domain: ", split(email, "@")[1])
       END;

-- COMMAND ----------

--This SQL code selects the "email" column from the "customers" table, and also includes a calculated column called "domain_category". The calculated column is generated by applying the "site_type" function to each email value. The result is a list of emails and their corresponding domain categories.

SELECT email, site_type(email) as domain_category
FROM customers

-- COMMAND ----------

--The SQL code is dropping two functions, namely get_url and site_type. This means that the functions will be removed from the database and can no longer be used or referenced.

DROP FUNCTION get_url;
DROP FUNCTION site_type;

-- COMMAND ----------


