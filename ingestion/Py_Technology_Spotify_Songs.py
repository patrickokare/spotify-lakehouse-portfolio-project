# Databricks notebook source
# ------------------------------------------------------------------------------------
# Title: Spotify Song Ingestion Pipeline (Web API)
# Description: Extracts songs from a Spotify playlist using Spotipy, lands raw data in ADLS,
#              applies transformations, and loads it into the raw and cleansed Delta Lake zones.
# Author: Patrick Okare
# Date: 2025-07-15
# ------------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run "/Repos/waleokare@gmail.com/databricks-project/Projects/Technology/Spotify/includes/01_SpotipyCredentials"

# COMMAND ----------

# MAGIC %run "/Repos/waleokare@gmail.com/databricks-project/Projects/Setup/02_ADLS_Container_Paths"

# COMMAND ----------

# MAGIC %run "/Repos/waleokare@gmail.com/databricks-project/Projects/Setup/03_Project_Parameters"

# COMMAND ----------

# MAGIC %run "/Repos/waleokare@gmail.com/databricks-project/Projects/Setup/04_Configurations"

# COMMAND ----------

import spotipy
from pyspark.sql.functions import lit, col, to_timestamp, concat_ws, sha2
from pyspark.sql.types import StructField, StructType, StringType, LongType

# COMMAND ----------

# Format the timestamp in the desired format
ingestion_date = est_now.strftime("%Y-%m-%d_%H:%M:%S:%f")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🟢 Source To Landing Zone

# COMMAND ----------

sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# COMMAND ----------

SourceQuery = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF"
playlist_uri = SourceQuery.split("/")[-1]

# COMMAND ----------

spotify_dataset = sp.playlist_tracks(playlist_uri)

# COMMAND ----------

song_list = []
for row in spotify_dataset['items']:
    song_id = row['track']['id']
    song_name = row['track']['name']
    song_duration = row['track']['duration_ms']
    song_url = row['track']['external_urls']['spotify']
    song_popularity = row['track']['popularity']
    song_added = row['added_at']
    album_id = row['track']['album']['id']
    artist_id = row['track']['album']['artists'][0]['id']

    song_element = {
        'song_id': song_id,
        'song_name': song_name,
        'duration_ms': song_duration,
        'url': song_url,
        'popularity': song_popularity,
        'song_added': song_added,
        'album_id': album_id,
        'artist_id': artist_id
    }
    song_list.append(song_element)

# COMMAND ----------

songs_landing_df = spark.createDataFrame(song_list)

# COMMAND ----------

songs_landing_df.coalesce(1).write.mode("overwrite").json(
    f"{landing_folder_path}{p_Domain}/{p_ProjectName}/{p_DataSourceName}/{p_EntityName}_{ingestion_date}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🟡 Landing To Raw Zone

# COMMAND ----------

songs_path = f"{landing_folder_path}{p_Domain}/{p_ProjectName}/{p_DataSourceName}/{p_EntityName}_{ingestion_date}"

songs_schema = StructType([
    StructField("album_id", StringType(), False),
    StructField("artist_id", StringType(), False),
    StructField("duration_ms", LongType(), False),
    StructField("popularity", LongType(), False),
    StructField("song_added", StringType(), False),
    StructField("song_id", StringType(), False),
    StructField("song_name", StringType(), False),
    StructField("url", StringType(), False)
])

songs_raw_df = spark.read.json(path=songs_path, schema=songs_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔵 Data Transformations - Applying Business Rules

# COMMAND ----------

hash_columns = [
    'album_id', 'artist_id', 'duration_ms', 'popularity',
    'song_added', 'song_id', 'song_name', 'source_system', 'ingestion_date'
]

songs_raw_df = songs_raw_df.drop('url') \
    .drop_duplicates(subset=['song_id']) \
    .withColumn("song_added", to_timestamp(col("song_added"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
    .withColumn("source_system", lit(p_DataSourceName)) \
    .withColumn("song_added", to_timestamp(col("song_added"), "yyyy-MM-dd")) \
    .withColumn("ingestion_date", lit(ingestion_date)) \
    .withColumn("HASH_ID", sha2(concat_ws("-", *[col(c) for c in hash_columns]), 256))

# COMMAND ----------

songs_raw_df.write.format("parquet") \
    .mode("overwrite") \
    .option('path', f"{raw_folder_path}{p_Domain}/{p_ProjectName}/{p_DataSourceName}/{p_EntityName}") \
    .saveAsTable("spotify_raw.raw_ext_songs")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🟣 Raw Zone To Cleansed Zone (Upsert using MERGE INTO)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO spotify_cleansed.songs AS tgt
# MAGIC USING spotify_raw.raw_ext_songs AS src
# MAGIC ON tgt.song_id = src.song_id
# MAGIC WHEN MATCHED AND src.HASH_ID <> tgt.HASH_ID THEN
# MAGIC   UPDATE SET tgt.album_id = src.album_id,
# MAGIC              tgt.artist_id = src.artist_id,
# MAGIC              tgt.duration_ms = src.duration_ms,
# MAGIC              tgt.popularity = src.popularity,
# MAGIC              tgt.song_added = src.song_added,
# MAGIC              tgt.song_name = src.song_name,
# MAGIC              tgt.source_system = src.source_system,
# MAGIC              tgt.ingestion_date = src.ingestion_date,
# MAGIC              tgt.HASH_ID = src.HASH_ID
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (song_id, album_id, artist_id, duration_ms, popularity, song_added, song_name, source_system, ingestion_date, HASH_ID)
# MAGIC   VALUES (src.song_id, src.album_id, src.artist_id, src.duration_ms, src.popularity, src.song_added, src.song_name, src.source_system, src.ingestion_date, src.HASH_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧱 (Optional) Create Externally Managed Delta Table in Cleansed Zone

# COMMAND ----------

# songs_raw_df.write.format("delta").mode("overwrite") \
#     .option('path', '/mnt/datamladls26/lake/cleansed/technology/spotify/web-api/songs') \
#     .saveAsTable("spotify_cleansed.songs")

# COMMAND ----------

dbutils.notebook.exit("Success")
