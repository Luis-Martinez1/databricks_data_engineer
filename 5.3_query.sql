-- Databricks notebook source
-- This SQL code retrieves the number of songs released by each artist each year from the table "prepared_song_data_lm2". The results are sorted in descending order by the number of songs and then by year. The "WHERE" clause filters out any records where the year is less than or equal to 0. The columns returned in the result set are the artist's name, the number of songs released, and the year.


%sql
-- Which artists released the most songs each year?
SELECT
  artist_name,
  count(artist_name)
AS
  num_songs,
  year
FROM
  prepared_song_data_lm2
WHERE
  year > 0
GROUP BY
  artist_name,
  year
ORDER BY
  num_songs DESC,
  year DESC


-- COMMAND ----------

-- The code retrieves song information from the "prepared_song_data_lm2" table. It selects the columns "artist_name", "title", and "tempo". The query filters the results by selecting songs with a time signature of 4 and a tempo between 100 and 140. This code is used to find songs suitable for a DJ list with a specific time signature and tempo range.




-- Find songs for your DJ list
 SELECT
   artist_name,
   title,
   tempo
 FROM
   prepared_song_data_lm2
 WHERE
   time_signature = 4
   AND
   tempo between 100 and 140;

-- COMMAND ----------


