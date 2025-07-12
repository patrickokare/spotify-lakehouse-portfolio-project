# Databricks notebook source
spark.sql(f"""
              CREATE TABLE IF NOT EXISTS spotify_modelled.SongsPopularityAgg
              (
              ID INT,
              SongID STRING,
              Songs STRING,
              Artists STRING,
              Albums STRING,
              PopularityRatings INT,
              HASH_ID STRING
              )
              USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW vSongPopularityAgg
            AS
            
               SELECT ROW_NUMBER() OVER (ORDER BY MAX(s.popularity) DESC) AS ID, 
                s.song_id AS SongID,
                s.song_name AS Songs,  
                ar.artist_name AS Artists, 
                a.album_name AS Albums,  
                s.popularity AS PopularityRatings,
                sha2(concat_ws("-", s.song_id ,s.song_name, ar.artist_name, a.album_name,  s.popularity ,
                MAX(s.popularity)), 256) AS HASH_ID
            FROM spotify_cleansed.songs s
            INNER JOIN spotify_cleansed.albums a ON a.album_id = s.album_id
            INNER JOIN spotify_cleansed.artists ar ON ar.artist_id = s.artist_id
            GROUP BY s.song_id, s.song_name, ar.artist_name, a.album_name, s.popularity, a.album_id, a.album_name, ar.artist_name
""")

# COMMAND ----------

spark.sql(f"""
    MERGE INTO spotify_modelled.SongsPopularityAgg AS target
    USING vSongPopularityAgg AS source
    ON target.SongID = source.SongID
    WHEN MATCHED AND target.HASH_ID <> source.HASH_ID THEN
        UPDATE SET
            target.Songs = source.Songs,
            target.Artists = source.Artists,
            target.Albums = source.Albums,
            target.PopularityRatings = source.PopularityRatings,
            target.HASH_ID = source.HASH_ID
    WHEN NOT MATCHED THEN
        INSERT (ID, SongID, Songs, Artists, Albums, PopularityRatings, HASH_ID)
        VALUES (source.ID, source.SongID, source.Songs, source.Artists, source.Albums, source.PopularityRatings, source.HASH_ID)
""")

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC SELECT * FROM spotify_modelled.SongsPopularityAgg
# MAGIC
# MAGIC