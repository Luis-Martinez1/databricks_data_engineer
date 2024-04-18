# Databricks notebook source
print("Hello World!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello world from SQL!"

# COMMAND ----------

# MAGIC %md
# MAGIC # Title 1
# MAGIC ## Title 2
# MAGIC ### Title 3
# MAGIC
# MAGIC text with a **bold** and *italicized* in it.
# MAGIC
# MAGIC Ordered list
# MAGIC 1. first
# MAGIC 1. second
# MAGIC 1. third
# MAGIC
# MAGIC Unordered list
# MAGIC * coffee
# MAGIC * tea
# MAGIC * milk
# MAGIC
# MAGIC
# MAGIC Images:
# MAGIC ![Associate-badge](https://www.databricks.com/wp-content/uploads/2022/04/associate-badge-eng.svg)
# MAGIC
# MAGIC And of course, tables:
# MAGIC
# MAGIC | user_id | user_name |
# MAGIC |---------|-----------|
# MAGIC |    1    |    Adam   |
# MAGIC |    2    |    Sarah  |
# MAGIC |    3    |    John   |
# MAGIC
# MAGIC Links (or Embedded HTML): <a href="https://docs.databricks.com/notebooks/notebooks-manage.html" target="_blank"> Managing Notebooks documentation</a>

# COMMAND ----------

# MAGIC %run ../Includes/Setup

# COMMAND ----------

print(full_name)

# COMMAND ----------

# This code is listing the contents of the directory '/databricks-datasets'.
%fs ls '/databricks-datasets'

# COMMAND ----------


#The code is calling the help() function on the dbutils object. This function will display information and documentation about how to use the functionality provided by dbutils.
dbutils.help()

# COMMAND ----------

#The code dbutils.fs.help() is using the dbutils module in Databricks to access file system operations. fs.help() is a method that displays the help documentation for the file system operations available in Databricks.
dbutils.fs.help()

# COMMAND ----------

# The code is using the dbutils.fs.ls() function to list all the files and directories in the '/databricks-datasets' directory. It then prints out the list of files and directories retrieved.
files = dbutils.fs.ls('/databricks-datasets')
print(files)

# COMMAND ----------

#The code is using the display function to show the contents of the files variable. It is assumed that files is a collection of some sort (like a list or an array) that contains file names. The purpose of this code is to display the file names to the user.
display(files)

# COMMAND ----------

print ("hello data bricks")

# COMMAND ----------


