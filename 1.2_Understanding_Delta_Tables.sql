-- Databricks notebook source
'''This SQL code creates a table called "employees_LM1" with three columns: "id" of type integer, "name" of type string, and "salary" of type double.'''
CREATE TABLE employees_LM1
  (id INT, name STRING, salary DOUBLE);

-- COMMAND ----------

'''   The code is inserting rows of data into a table named "employees_LM". Each row consists of three values: an employee ID, an employee name, and a salary. The values are specified in a comma-separated list of tuples. The code will insert six rows into the table with the provided data.   ''' 
INSERT INTO employees_LM
VALUES 
  (1, "Adam", 3500.0),
  (2, "Sarah", 4020.5),
  (3, "John", 2999.3),
  (4, "Thomas", 4000.3),
  (5, "Anna", 2500.0),
  (6, "Kim", 6200.3)

-- COMMAND ----------

SELECT * FROM employees_LM order by name

-- COMMAND ----------

'''  The code DESCRIBE DETAIL employees_LM is used in SQL to retrieve the details or structure of the table called employees_LM. It returns information about the columns in the table, such as the column name, data type, and any constraints or indexes applied to the columns. This information can be useful when querying or working with the table, as it helps to understand the structure of the data.  '''
DESCRIBE DETAIL employees_LM

-- COMMAND ----------

'''   The code is using the ls command to list the contents of a directory. The directory being listed is dbfs:/user/hive/warehouse/employees_lm.  ''''
%fs ls 'dbfs:/user/hive/warehouse/employees_lm' 

-- COMMAND ----------

''' This SQL code updates the "salary" column in the "employees_lm" table. It increases the value of the "salary" column by 1000 for all rows where the "name" starts with the letter "A" '''
UPDATE employees_lm 
SET salary = salary + 1000
WHERE name LIKE "A%"

-- COMMAND ----------

SELECT * FROM employees_LM order by name

-- COMMAND ----------

''' The code is written in F# and it uses the ls command to list the contents of a directory. The directory being listed is dbfs:/user/hive/warehouse/employees_lm. '''
%fs ls 'dbfs:/user/hive/warehouse/employees_lm'

-- COMMAND ----------

''' The code executed a SQL command to describe the structure and properties of the table "employees_lm". It provides information about the column names, data types, and other attributes of the tables columns  '''
DESCRIBE DETAIL employees_lm

-- COMMAND ----------

SELECT * FROM employees_lm

-- COMMAND ----------

'''The code is a SQL query that is used to describe the structure of a table called employees_lm in a database. The DESCRIBE statement retrieves information about the columns of a table, including the column name, data type, and any additional attributes associated with each column (such as whether it is a primary key or not). So, this code will display the details of the columns in the employees_lm table.'''
DESCRIBE HISTORY employees_lm

-- COMMAND ----------

'''  The code is using the ls command in Apache Spark to list the contents of a delta log file located in the directory dbfs:/user/hive/warehouse/employees_lm/_delta_log. This allows the user to view the metadata and changes recorded in the delta log.   '''
%fs ls 'dbfs:/user/hive/warehouse/employees_lm/_delta_log'

-- COMMAND ----------

'''   The code reads the contents of a delta log file located at the path dbfs:/user/hive/warehouse/employees_lm/_delta_log/00000000000000000002.json and retrieves the first few lines of the file.  '''
%fs head 'dbfs:/user/hive/warehouse/employees_lm/_delta_log/00000000000000000002.json'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print("hello databricks")
