# Databricks notebook source
# ------------------------------------------------------------------------------------
# Title: Spotify Artist Ingestion Pipeline (Web API)
# Description: Extract artists from a Spotify playlist using Spotipy, land data in ADLS,
#              transform and write into raw & cleansed zones using Delta Lake format.
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
from pyspark.sql.types import StructField, StructType, StringType

# COMMAND ----------

# Format the timestamp in the desired format
ingestion_date = est_now.strftime("%Y-%m-%d_%H:%M:%S:%f")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🟢 Source To Landing Zone

# COMMAND ----------

sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# COMMAND ----------

SourceLink = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF"
playlist_uri = SourceLink.split("/")[-1]

# COMMAND ----------

spotify_dataset = sp.playlist_tracks(playlist_uri)

# COMMAND ----------

artist_list = []
for row in spotify_dataset['items']:
    for key, value in row.items():
        if key == "track":
            for artist in value['artists']:
                artist_dict = {
                    'artist_id': artist['id'],
                    'artist_name': artist['name'],
                    'external_url': artist['href']
                }
                artist_list.append(artist_dict)

# COMMAND ----------

artists_landing_df = spark.createDataFrame(artist_list)

# COMMAND ----------

artists_landing_df.coalesce(1).write.mode("overwrite").json(
    f"{landing_folder_path}{p_Domain}/{p_ProjectName}/{p_DataSourceName}/{p_EntityName}_{ingestion_date}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🟡 Landing To Raw Zone

# COMMAND ----------

artists_path = f"{landing_folder_path}{p_Domain}/{p_ProjectName}/{p_DataSourceName}/{p_EntityName}_{ingestion_date}"

artists_schema = StructType([
    StructField("artist_id", StringType(), False),
    StructField("artist_name", StringType(), False),
    StructField("external_url", StringType(), False)
])

artists_raw_df = spark.read.json(path=artists_path, schema=artists_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔵 Data Transformations - Applying Business Rules

# COMMAND ----------

hash_columns = ['artist_id', 'artist_name', 'source_system', 'ingestion_date']

artists_raw_df = artists_raw_df.drop('external_url') \
    .drop_duplicates(subset=['artist_id']) \
    .withColumn("source_system", lit(p_DataSourceName)) \
    .withColumn("ingestion_date", lit(ingestion_date)) \
    .withColumn("HASH_ID", sha2(concat_ws("-", *[col(c) for c in hash_columns]), 256))

# COMMAND ----------

artists_raw_df.write.format("parquet") \
    .mode("overwrite") \
    .option('path', f"{raw_folder_path}{p_Domain}/{p_ProjectName}/{p_DataSourceName}/{p_EntityName}") \
    .saveAsTable("spotify_raw.raw_ext_artists")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🟣 Raw Zone To Cleansed Zone (Upsert using MERGE INTO)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO spotify_cleansed.artists AS tgt
# MAGIC USING spotify_raw.raw_ext_artists AS src
# MAGIC ON tgt.artist_id = src.artist_id
# MAGIC WHEN MATCHED AND src.HASH_ID <> tgt.HASH_ID THEN
# MAGIC   UPDATE SET tgt.artist_name = src.artist_name,
# MAGIC              tgt.source_system = src.source_system,
# MAGIC              tgt.ingestion_date = src.ingestion_date,
# MAGIC              tgt.HASH_ID = src.HASH_ID
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (artist_id, artist_name, source_system, ingestion_date, HASH_ID)
# MAGIC   VALUES (src.artist_id, src.artist_name, src.source_system, src.ingestion_date, src.HASH_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧱 (Optional) Create Externally Managed Delta Table in Cleansed Zone

# COMMAND ----------

# artists_raw_df.write.format("delta").mode("overwrite") \
#     .option('path', '/mnt/datamladls26/lake/cleansed/technology/spotify/web-api/artists') \
#     .option("mergeSchema", "true") \
#     .saveAsTable("spotify_cleansed.artists")

# COMMAND ----------

dbutils.notebook.exit("Success")
