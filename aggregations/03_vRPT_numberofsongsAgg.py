
# Databricks notebook source
# ------------------------------------------------------------------------------------
# Author: Patrick Okare
# Date: 2025-07-15
# Project: Spotify Analytics on Lakehouse (Medallion Architecture)
# File: aggregations/03_vRPT_numberofsongsAgg.py
# Description: This script generates an aggregation showing the number of songs 
#              released per album by each artist, along with popularity ranking and 
#              change tracking via HASH_ID for idempotent Delta Lake merges.
# ------------------------------------------------------------------------------------

# COMMAND ----------

# Step 1: Create the destination table if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS spotify_modelled.NumOfSongsAgg
    (
        ID INT,
        ArtistName STRING,
        AlbumName STRING,
        NumberOfSongs INT,
        PopularityRank INT,
        HASH_ID STRING
    )
    USING DELTA
""")

# COMMAND ----------

# Step 2: Create temp view for aggregating songs per album per artist
# - Adds popularity rank (descending) based on number of songs
# - Adds hash for merge logic

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW vFileNameAgg AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY COUNT(s.song_id) DESC) AS ID,
            ar.artist_name AS ArtistName,
            a.album_name AS AlbumName,
            COUNT(s.song_id) AS NumberOfSongs,
            RANK() OVER (PARTITION BY ar.artist_name ORDER BY COUNT(s.song_id) DESC) AS PopularityRank,
            sha2(concat_ws("-", ar.artist_name, a.album_name, COUNT(s.song_id)), 256) AS HASH_ID
        FROM hive_metastore.spotify_cleansed.songs s
        INNER JOIN hive_metastore.spotify_cleansed.albums a ON s.album_id = a.album_id
        INNER JOIN hive_metastore.spotify_cleansed.artists ar ON s.artist_id = ar.artist_id
        GROUP BY ar.artist_name, a.album_name
""")

# COMMAND ----------

# Step 3: Merge the result into the Delta table
spark.sql(f"""
    MERGE INTO spotify_modelled.NumOfSongsAgg AS target
    USING vFileNameAgg AS source
    ON target.ArtistName = source.ArtistName AND target.AlbumName = source.AlbumName
    WHEN MATCHED AND target.HASH_ID <> source.HASH_ID THEN
        UPDATE SET
            target.ID = source.ID,
            target.NumberOfSongs = source.NumberOfSongs,
            target.PopularityRank = source.PopularityRank,
            target.HASH_ID = source.HASH_ID
    WHEN NOT MATCHED THEN
        INSERT (ID, ArtistName, AlbumName, NumberOfSongs, PopularityRank, HASH_ID)
        VALUES (source.ID, source.ArtistName, source.AlbumName, source.NumberOfSongs, source.PopularityRank, source.HASH_ID)
""")

# COMMAND ----------

# Step 4: Preview the result
# MAGIC %sql
# MAGIC SELECT * FROM spotify_modelled.NumOfSongsAgg
# MAGIC ORDER BY NumberOfSongs DESC
