# Databricks notebook source
# ------------------------------------------------------------------------------------
# Author: Patrick Okare
# Date: 2025-07-15
# Project: Spotify Analytics on Lakehouse (Medallion Architecture)
# Description: This notebook creates and populates the AlbumSummaryAgg table,
#              providing a summary of albums ranked by the highest number of tracks.
#              It uses Delta Lake and implements change tracking via HASH_ID to
#              support incremental updates using MERGE logic.
# ------------------------------------------------------------------------------------

# COMMAND ----------

# Step 1: Create the target Delta table if it doesn't already exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS spotify_modelled.AlbumSummaryAgg
    (
        ID INT,
        AlbumID STRING,
        AlbumName STRING,
        ArtistName STRING,
        HighestNumberOfTracks INT,
        HASH_ID STRING
    )
    USING DELTA
""")

# COMMAND ----------

# Step 2: Create a temporary view with album summary
# - Assigns unique ID based on highest track count
# - Uses SHA2 hash for idempotent comparison in MERGE

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW vAlbumSummaryAgg AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY MAX(a.total_tracks) DESC) AS ID, 
            a.album_id AS AlbumID, 
            a.album_name AS AlbumName, 
            ar.artist_name AS ArtistName, 
            MAX(a.total_tracks) AS HighestNumberOfTracks,
            sha2(concat_ws("-", a.album_id, a.album_name, ar.artist_name, MAX(a.total_tracks)), 256) AS HASH_ID
        FROM hive_metastore.spotify_cleansed.albums a 
        INNER JOIN hive_metastore.spotify_cleansed.songs s ON a.album_id = s.album_id
        INNER JOIN hive_metastore.spotify_cleansed.artists ar ON ar.artist_id = s.artist_id
        GROUP BY a.album_id, a.album_name, ar.artist_name
        ORDER BY HighestNumberOfTracks DESC
""")

# COMMAND ----------

# Step 3: Merge view data into the modelled Delta table
# - Updates existing rows where HASH_ID has changed
# - Inserts new records when not matched

spark.sql(f"""
    MERGE INTO spotify_modelled.AlbumSummaryAgg AS target
    USING vAlbumSummaryAgg AS source
    ON target.AlbumID = source.AlbumID
    WHEN MATCHED AND target.HASH_ID <> source.HASH_ID THEN
        UPDATE SET
            target.ID = source.ID,
            target.AlbumName = source.AlbumName,
            target.ArtistName = source.ArtistName,
            target.HighestNumberOfTracks = source.HighestNumberOfTracks,
            target.HASH_ID = source.HASH_ID
    WHEN NOT MATCHED THEN
        INSERT (ID, AlbumID, AlbumName, ArtistName, HighestNumberOfTracks, HASH_ID)
        VALUES (source.ID, source.AlbumID, source.AlbumName, source.ArtistName, source.HighestNumberOfTracks, source.HASH_ID)
""")

# COMMAND ----------

# Step 4: Preview the final modelled data for album summaries
# Displays albums sorted by track count

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.spotify_modelled.albumsummaryagg
