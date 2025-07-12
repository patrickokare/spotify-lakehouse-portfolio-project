# Databricks notebook source
# MAGIC  %run "/Repos/waleokare@gmail.com/databricks-project/Projects/Technology/Spotify/includes/01_SpotipyCredentials"

# COMMAND ----------

# MAGIC %run "/Repos/waleokare@gmail.com/databricks-project/Projects/Setup/02_ADLS_Container_Paths"

# COMMAND ----------

# MAGIC %run "/Repos/waleokare@gmail.com/databricks-project/Projects/Setup/03_Project_Parameters"

# COMMAND ----------

# MAGIC %run "/Repos/waleokare@gmail.com/databricks-project/Projects/Setup/04_Configurations"

# COMMAND ----------

import spotipy
from pyspark.sql.functions import lit, col, to_timestamp, concat_ws, sha2
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType, LongType

# COMMAND ----------

# Format the timestamp in the desired format
ingestion_date = est_now.strftime("%Y-%m-%d_%H:%M:%S:%f")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Source To Landing Zone

# COMMAND ----------

sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)

# COMMAND ----------

SourceQuery = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF"

# COMMAND ----------

playlist_uri = SourceQuery.split("/")[-1]

# COMMAND ----------

spotify_dataset = sp.playlist_tracks(playlist_uri)

# COMMAND ----------

album_list = []
for row in spotify_dataset['items']:
    album_id = row['track']['album']['id']
    album_name = row['track']['album']['name']
    album_release_date = row['track']['album']['release_date']
    album_total_tracks = row['track']['album']['total_tracks']
    album_url = row['track']['album']['external_urls']['spotify']
    
    album_element = {'album_id':album_id,'name':album_name,'release_date':album_release_date,
                        'total_tracks':album_total_tracks,'url':album_url}
    album_list.append(album_element)

# COMMAND ----------

album_landing_df = spark.createDataFrame(album_list)

# COMMAND ----------

# Reduce the number of partitions to 1 before writing
album_landing_df.coalesce(1).write.mode("overwrite").json(f"{landing_folder_path}{p_Domain}/{p_ProjectName}/{p_DataSourceName}/{p_EntityName}_{ingestion_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Landing To Raw Zone

# COMMAND ----------

albums_path = f"{landing_folder_path}{p_Domain}/{p_ProjectName}/{p_DataSourceName}/{p_EntityName}_{ingestion_date}"

albums_schema = StructType([
    StructField("album_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("release_date", StringType(), False),
    StructField("total_tracks", LongType(), False),
    StructField("url", StringType(), False)
])

albums_raw_df = spark.read.json(path=albums_path, schema=albums_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Transformations - Applying Business Rules
# MAGIC #### This list comprehension converts the column names in hash_columns

# COMMAND ----------

# Define the column list to concatenate for the hash key
hash_columns = ['album_id', 'album_name', 'release_date', 'total_tracks']

# COMMAND ----------

albums_raw_df = albums_raw_df.drop_duplicates(subset=['album_id']) \
                             .drop('url') \
                             .withColumnRenamed("name", "album_name") \
                             .withColumn("release_date", to_timestamp(col("release_date"), 'yyyy-MM-dd')) \
                             .withColumn("source_system", lit(p_DataSourceName)) \
                             .withColumn("ingestion_date", lit(ingestion_date)) \
                             .withColumn("HASH_ID", 
                                         sha2(concat_ws("-", *[col(c) for c in hash_columns]), 256))

# COMMAND ----------

albums_raw_df.write.format("parquet").mode("overwrite").option('path', f"{raw_folder_path}{p_Domain}/{p_ProjectName}/{p_DataSourceName}/{p_EntityName}").saveAsTable("spotify_raw.raw_ext_albums")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Raw Zone To Cleansed

# COMMAND ----------

# MAGIC  %sql
# MAGIC  --SET spark.databricks.delta.schema.autoMerge.enabled = true;
# MAGIC
# MAGIC
# MAGIC  MERGE INTO spotify_cleansed.albums AS tgt
# MAGIC USING spotify_raw.raw_ext_albums AS src
# MAGIC ON tgt.album_id = src.album_id
# MAGIC WHEN MATCHED AND src.HASH_ID <> tgt.HASH_ID THEN
# MAGIC   UPDATE SET tgt.album_name = src.album_name,
# MAGIC              tgt.release_date = src.release_date,
# MAGIC              tgt.total_tracks = src.total_tracks,
# MAGIC              tgt.source_system = src.source_system,
# MAGIC              tgt.ingestion_date = src.ingestion_date,
# MAGIC             tgt.HASH_ID  = src.HASH_ID
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (album_id, album_name, release_date, total_tracks, source_system, ingestion_date, HASH_ID)
# MAGIC   VALUES (src.album_id, src.album_name, src.release_date, src.total_tracks, src.source_system, src.ingestion_date, src.HASH_ID )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Externally Managed Delta Tables in Cleansed Zone

# COMMAND ----------

# albums_raw_df.write.format("delta").mode("overwrite").option('path', '/mnt/datamladls26/lake/cleansed/technology/spotify/web-api/albums').saveAsTable("spotify_cleansed.albums")

# COMMAND ----------

dbutils.notebook.exit("Success")