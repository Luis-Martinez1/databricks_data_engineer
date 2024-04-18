-- Databricks notebook source
-- The code DESCRIBE HISTORY employees_lm is used to retrieve the structure and metadata information about the table employees_lm including its column names, data types, and any applicable constraints. It provides a description of the table's history.
DESCRIBE HISTORY employees_lm

-- COMMAND ----------

-- The code is a SQL query that selects all columns and rows from a table called "employees_lm" at a specific version. The table has a versioning mechanism that allows accessing data from different points in time. In this case, the query retrieves the data at version 1 of the "employees_lm" table.
SELECT * 
FROM employees_lm VERSION AS OF 1

-- COMMAND ----------

SELECT * FROM employees@v1

-- COMMAND ----------

DELETE FROM employees_lm

-- COMMAND ----------

SELECT * FROM employees_lm

-- COMMAND ----------

--The code is restoring the table "employees_lm" to a previous version based on the version number specified (in this case, version 2). This means that any changes made to the table after version 2 will be undone, and the table will be reverted to its state at version 2.
RESTORE TABLE employees_lm TO VERSION AS OF 2 

-- COMMAND ----------

SELECT * FROM employees_lm

-- COMMAND ----------

--The SQL code DESCRIBE HISTORY employees_lm is used to describe the table schema, i.e., the structure of the table called employees_lm. The DESCRIBE HISTORY command is specific to certain database systems and is used to retrieve the historical metadata or history of changes made to the table, such as column modifications, data type changes, or other alterations.
DESCRIBE HISTORY employees_lm

-- COMMAND ----------

DESCRIBE DETAIL employees_lm

-- COMMAND ----------

-- The SQL code is used to optimize the table "employees_lm" by rearranging its data based on the "id" column using the Z-ordering technique. This process reorganizes the data to improve query performance and optimize storage utilization.
OPTIMIZE employees_lm
ZORDER BY id

-- COMMAND ----------

DESCRIBE DETAIL employees_lm

-- COMMAND ----------

DESCRIBE HISTORY employees_lm

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees_lm'

-- COMMAND ----------

--The code is performing a VACUUM operation on the "employees_lm" table. VACUUM is a command in SQL that reclaims disk space occupied by deleted tuples/rows and frees up unused space. It also improves query performance by optimizing table storage. "employees_lm" is the name of the table that is being vacuumed.
VACUUM employees_lm

-- COMMAND ----------

--The given code is written in FS (File System) syntax and is used to list the files and directories present in the specified path in the Databricks file system (DBFS).

--The code uses the "ls" command followed by the path 'dbfs:/user/hive/warehouse/employees_lm'.

--In this case, the code will list the files and directories present in the 'employees_lm' directory located in the 'warehouse' directory, which is contained within the 'hive' directory, which is located in the 'user' directory in the DBFS.

%fs ls 'dbfs:/user/hive/warehouse/employees_lm'

-- COMMAND ----------



-- COMMAND ----------

-- The code SET spark.databricks.delta.retentionDurationCheck.enabled = false; is used to disable the retention duration check in Spark Databricks Delta.

--By default, Delta checks the retention duration set on a table and ensures that it is not shorter than the amount of time needed to keep older versions of the data. It helps in preventing accidental data loss by validating the retention duration.

--However, sometimes for testing or debugging purposes, it may be necessary to disable this check. The mentioned code sets the configuration property spark.databricks.delta.retentionDurationCheck.enabled to false, indicating that the retention duration check should be turned off.
SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

-- The SQL code you provided is a VACUUM command with the following options:

--employees_lm is the name of the table or index you want to vacuum.
--RETAIN 0 HOURS specifies that no work hours should be retained for the vacuum operation.

--The VACUUM command in SQL is used to reorganize the physical storage of table or index data and minimize disk space usage. In this case, it would vacuum the employees_lm table and not retain any work hours for the operation.

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


