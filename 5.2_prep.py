# Databricks notebook source
# This SQL script consists of two main parts: creating a new table and inserting data into that table.

# Create Table:

# CREATE OR REPLACE TABLE prepared_song_data_lm2 (...): This command creates a new table named prepared_song_data_lm2 in the database. If the table already exists, it replaces the existing table. This ensures that you're always working with the most recent structure without manual intervention.
# The table structure is defined with the columns artist_id, artist_name, duration, release, tempo, time_signature, title, year, and processed_time, each specified with a data type (STRING for text-like fields, DOUBLE for numeric fields with decimal places, and TIMESTAMP for date-time values).
# Insert Data:

# The INSERT INTO prepared_song_data_lm2 statement is used to populate the prepared_song_data_lm2 table with data.
# SELECT [...] FROM raw_song_data: This portion retrieves data from another table named raw_song_data. It selects columns that match the structure of the prepared_song_data_lm2 table.
# current_timestamp(): In addition to the columns sourced from raw_song_data, the script adds a timestamp at the moment of insertion for each row using current_timestamp(). This is inserted into the processed_time column, allowing you to track when each record was processed.
# The overall purpose of this script is to transfer and transform data from a raw format (raw_song_data table) into a more prepared state (prepared_song_data_lm2 table), adding a processing timestamp to each record for tracking or auditing purposes. This could be a part of a data pipeline in a data warehousing solution, where data is cleaned and structured before being used for analysis or reporting.



%sql
CREATE OR REPLACE TABLE
  prepared_song_data_lm2 (
    artist_id STRING,
    artist_name STRING,
    duration DOUBLE,
    release STRING,
    tempo DOUBLE,
    time_signature DOUBLE,
    title STRING,
    year DOUBLE,
    processed_time TIMESTAMP
  );
 
INSERT INTO
  prepared_song_data_lm2
SELECT
  artist_id,
  artist_name,
  duration,
  release,
  tempo,
  time_signature,
  title,
  year,
  current_timestamp()
FROM
  raw_song_data
