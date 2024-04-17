-- Databricks notebook source
CREATE TABLE employees_LM1
  (id INT, name STRING, salary DOUBLE);

-- COMMAND ----------

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

DESCRIBE DETAIL employees_LM

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees_lm' 

-- COMMAND ----------

UPDATE employees_lm 
SET salary = salary + 1000
WHERE name LIKE "A%"

-- COMMAND ----------

SELECT * FROM employees_LM order by name

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees_lm'

-- COMMAND ----------

DESCRIBE DETAIL employees_lm

-- COMMAND ----------

SELECT * FROM employees_lm

-- COMMAND ----------

DESCRIBE HISTORY employees_lm

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees_lm/_delta_log'

-- COMMAND ----------

-- MAGIC %fs head 'dbfs:/user/hive/warehouse/employees_lm/_delta_log/00000000000000000002.json'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print("hello databricks")
