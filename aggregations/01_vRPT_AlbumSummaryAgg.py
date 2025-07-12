# Databricks notebook source
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

spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW vAlbumSummaryAgg
            AS
            
                 SELECT ROW_NUMBER() OVER (ORDER BY MAX(a.total_tracks) DESC) AS ID, 
               a.album_id AS AlbumID, a.album_name AS AlbumName, ar.artist_name AS ArtistName, MAX(a.total_tracks) AS HighestNumberOfTracks,
                 sha2(concat_ws("-", a.album_id, a.album_name, ar.artist_name, MAX(a.total_tracks)), 256) AS HASH_ID
                FROM hive_metastore.spotify_cleansed.albums a 
                INNER JOIN hive_metastore.spotify_cleansed.songs s ON a.album_id = s.album_id
                INNER JOIN hive_metastore.spotify_cleansed.artists ar ON ar.artist_id = s.artist_id
                GROUP BY a.album_id, a.album_name, ar.artist_name
                ORDER BY HighestNumberOfTracks DESC
""")

# COMMAND ----------

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

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.spotify_modelled.albumsummaryagg