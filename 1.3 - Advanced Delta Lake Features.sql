-- Databricks notebook source
DESCRIBE HISTORY employees_lm

-- COMMAND ----------

SELECT * 
FROM employees_lm VERSION AS OF 1

-- COMMAND ----------

SELECT * FROM employees@v1

-- COMMAND ----------

DELETE FROM employees_lm

-- COMMAND ----------

SELECT * FROM employees_lm

-- COMMAND ----------

RESTORE TABLE employees_lm TO VERSION AS OF 2 

-- COMMAND ----------

SELECT * FROM employees_lm

-- COMMAND ----------

DESCRIBE HISTORY employees_lm

-- COMMAND ----------

DESCRIBE DETAIL employees_lm

-- COMMAND ----------

OPTIMIZE employees_lm
ZORDER BY id

-- COMMAND ----------

DESCRIBE DETAIL employees_lm

-- COMMAND ----------

DESCRIBE HISTORY employees_lm

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees_lm'

-- COMMAND ----------

VACUUM employees_lm

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees_lm'

-- COMMAND ----------



-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

VACUUM employees_lm RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees_lm'

-- COMMAND ----------

SELECT * FROM employees@v1

-- COMMAND ----------

DROP TABLE employees_lm

-- COMMAND ----------

SELECT * FROM employees_lm

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees_lm'

-- COMMAND ----------


