# Databricks notebook source
# The code defines a function path_exists(path) that checks if a given path exists in a Databricks file system (DBFS).

# The function accepts one argument, path, which is a string representing the file or directory path you wish to check the existence of.

# Inside the function, it tries to list the contents of the given path using dbutils.fs.ls(path). The dbutils.fs.ls method is a Databricks utility that lists the files and directories in the specified path. If the path exists and is accessible, this method executes without error.

# If the path is valid and accessible, the function immediately returns True, indicating the path exists.

# If an exception occurs, the function checks the exception message to see if it includes 'java.io.FileNotFoundException', which indicates that the path does not exist or is not accessible. In this case, the function returns False.

# If the exception is due to any other reason, it re-raises the exception, allowing it to propagate upwards so that the caller can handle it as necessary. This ensures that only the specific case of a non-existent path is silently handled, while all other exceptions are flagged for further investigation.

# Essentially, this function is a utility to safely check for the existence of a path in DBFS, differentiating between paths that don't exist and other potential errors that might need further attention.

def path_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------

# This Python function, download_dataset, is designed to download a dataset from a source location to a target location, typically within a filesystem or database that is accessible via the dbutils utility. Here is a concise explanation of its components:

# Function Definition: The function is named download_dataset and requires two arguments: source and target, which represent the source location (where the files are currently stored) and the target location (where the files should be downloaded to), respectively.

# Listing Files: The function starts by listing all the files in the source location using dbutils.fs.ls(source). This method returns a list of file information, where each item in the list is an object that includes properties of the file, such as its name.

# Looping Through Files: For each file object in the list obtained from the source location, the function constructs the full source and target paths by appending the file name (f.name) to the source and target directory paths, respectively.

# Checking Target Path: Before copying a file, the function checks if the target path already exists using an undefined function path_exists(target_path). This step ensures that the function doesn't unnecessarily copy files that are already present at the target location.

# Copying Files: If the target path does not exist (path_exists returns False), the function prints a message indicating it is copying the file, and then utilizes dbutils.fs.cp(source_path, target_path, True) to copy the file from the source to the target location. The third parameter True in dbutils.fs.cp likely indicates an overwrite flag, which means if the target file somehow exists (despite passing the path_exists check), it will be overwritten.

# Please note, the function path_exists(target_path) is referenced but not defined within this code snippet, suggesting it is either defined elsewhere in the program or is a placeholder for a built-in or third-party library function that checks for the existence of a given file path. Also, dbutils is a utility, often associated with Databricks environments, for interacting with the filesystem in a distributed computing environment.

def download_dataset(source, target):
    files = dbutils.fs.ls(source)

    for f in files:
        source_path = f"{source}/{f.name}"
        target_path = f"{target}/{f.name}"
        if not path_exists(target_path):
            print(f"Copying {f.name} ...")
            dbutils.fs.cp(source_path, target_path, True)

# COMMAND ----------

#This code sets the value of the data_source_uri variable to a Blob storage URI. It then sets the value of the dataset_bookstore variable to a Databricks file system (DBFS) URI. Finally, it uses the spark.conf.set() method to set a Spark configuration property with the key dataset.bookstore and its value as the dataset_bookstore variable.

data_source_uri = "wasbs://course-resources@dalhussein.blob.core.windows.net/datasets/bookstore/v1/"
dataset_bookstore = 'dbfs:/mnt/demo-datasets/bookstore'
spark.conf.set(f"dataset.bookstore", dataset_bookstore)

# COMMAND ----------

# The code defines a function called get_index that takes a parameter dir representing a directory.

# Inside the function, it uses dbutils.fs.ls(dir) to get a list of files in the given directory.

# It then initializes a variable index to 0.

# Next, it checks if the files list is not empty, i.e., if there are files in the directory.

# If there are files, it finds the file with the highest name (based on lexicographic ordering) by using max(files).name.

# It then extracts the file name without the extension by using rsplit('.', maxsplit=1)[0]. This splits the file name at the last dot and returns the part before the dot.

# It converts this extracted file name to an integer using int().

# Finally, it increments the index by 1 to get the next index and returns it.

# This function essentially finds the highest index used in the filenames in the given directory, and returns the next available index.

def get_index(dir):
    files = dbutils.fs.ls(dir)
    index = 0
    if files:
        file = max(files).name
        index = int(file.rsplit('.', maxsplit=1)[0])
    return index+1

# COMMAND ----------

# This Python code is part of a data processing workflow involving streaming data, specifically designed to work within a Databricks environment due to the use of dbutils. The goal is to incrementally load parquet files from a streaming directory to a raw directory for further processing. It manages the transfer of these files based on a naming convention and keeps track of the latest file loaded. Here's a concise explanation of each part:

# Global Variables
# streaming_dir: The directory path (in a variable dataset_bookstore) where new streaming data files are stored.
# raw_dir: The directory path where the files from streaming_dir are to be copied for raw data storage.
# load_file(current_index)
# Purpose: Copies a file based on the current_index from the streaming directory to the raw directory.
# latest_file: Constructs the file name to be copied using current_index, formatted to have leading zeros (e.g., "01.parquet").
# It prints a message indicating which file is being loaded and uses dbutils.fs.cp() function to copy the file from streaming_dir to raw_dir.
# load_new_data(all=False)
# Controls the loading of new data based on the provided all argument.
# Arguments:
# all: A boolean flag; when True, load all remaining files until the index reaches 10. If False, load only the next file.
# Functionality:
# Determines the next index of the file to be loaded by calling get_index(raw_dir), a function not defined in the code snippet. This function likely calculates the index by inspecting what has already been loaded into the raw_dir.
# Checks if all files up to index 10 have been loaded. If so, it prints a message stating there's no more data to load.
# If all is True and the index is not beyond 10, it enters a loop to load all files from the current index up to index 10.
# If all is False, it loads the next file (based on index) into the raw_dir.
# Missing Elements and Assumptions
# The get_index() function is referenced but not defined in the provided code. Presumably, this function calculates the current index by examining the contents of raw_dir.
# The actual logic for determining the end condition (index >= 10) is somewhat arbitrary without seeing how get_index() works. It assumes a fixed dataset size or a particular stopping criterion.
# Error handling, such as for failed file copies, is not present in this snippet.
# It assumes the availability and consistent availability of dbutils; this code is specific to Databricks environments.
# In summary, this code snippet is designed for incremental loading of streaming data from a designated directory into a raw data storage directory, with the capability to load either one file at a time or all remaining files in a controlled batch operation.


# Structured Streaming
streaming_dir = f"{dataset_bookstore}/orders-streaming"
raw_dir = f"{dataset_bookstore}/orders-raw"

def load_file(current_index):
    latest_file = f"{str(current_index).zfill(2)}.parquet"
    print(f"Loading {latest_file} file to the bookstore dataset")
    dbutils.fs.cp(f"{streaming_dir}/{latest_file}", f"{raw_dir}/{latest_file}")

    
def load_new_data(all=False):
    index = get_index(raw_dir)
    if index >= 10:
        print("No more data to load\n")

    elif all == True:
        while index <= 10:
            load_file(index)
            index += 1
    else:
        load_file(index)
        index += 1

# COMMAND ----------

# This code is for loading JSON files into a bookstore dataset.

# The code defines two directories, streaming_orders_dir and streaming_books_dir, which are the paths to the streaming orders and books directories in the bookstore dataset.

# It also defines two more directories, raw_orders_dir and raw_books_dir, which are the paths to the raw orders and books directories in the bookstore dataset.

# The function load_json_file(current_index) takes an index as input and loads the corresponding JSON files from the streaming orders and books directories to the raw orders and books directories in the bookstore dataset using dbutils.fs.cp().

# The function load_new_json_data(all=False) is used to load new JSON data. It first checks the current index by calling the get_index() function. If the index is greater than or equal to 10, it prints "No more data to load".

# If all is true, it enters a while loop and repeatedly calls the load_json_file() function to load all the JSON files from the streaming directories to the raw directories until the index reaches 10. Otherwise, it calls load_json_file() only once and increments the index by 1.


# DLT
streaming_orders_dir = f"{dataset_bookstore}/orders-json-streaming"
streaming_books_dir = f"{dataset_bookstore}/books-streaming"

raw_orders_dir = f"{dataset_bookstore}/orders-json-raw"
raw_books_dir = f"{dataset_bookstore}/books-cdc"

def load_json_file(current_index):
    latest_file = f"{str(current_index).zfill(2)}.json"
    print(f"Loading {latest_file} orders file to the bookstore dataset")
    dbutils.fs.cp(f"{streaming_orders_dir}/{latest_file}", f"{raw_orders_dir}/{latest_file}")
    print(f"Loading {latest_file} books file to the bookstore dataset")
    dbutils.fs.cp(f"{streaming_books_dir}/{latest_file}", f"{raw_books_dir}/{latest_file}")

    
def load_new_json_data(all=False):
    index = get_index(raw_orders_dir)
    if index >= 10:
        print("No more data to load\n")

    elif all == True:
        while index <= 10:
            load_json_file(index)
            index += 1
    else:
        load_json_file(index)
        index += 1

# COMMAND ----------

#This code is calling the function download_dataset with two arguments: data_source_uri and dataset_bookstore. The purpose of this function is to download a dataset from a given data source URI and store it in the dataset bookstore.

download_dataset(data_source_uri, dataset_bookstore)
