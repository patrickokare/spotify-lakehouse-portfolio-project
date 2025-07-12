# Databricks notebook source
# MAGIC %md
# MAGIC ### cREATION OF eXTERANLLY Unmanaged Tbales

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS spotify_raw
# MAGIC LOCATION "/mnt/datamladls26/lake/raw/technology/spotify/web-api/"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS spotify_cleansed
# MAGIC LOCATION "/mnt/datamladls26/lake/cleansed/technology/spotify/web-api/"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS spotify_modelled
# MAGIC LOCATION "/mnt/datamladls26/lake/modelled/technology/spotify/web-api/"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE spotify_raw.raw_ext_albums
# MAGIC (
# MAGIC album_id STRING, 
# MAGIC name STRING, 
# MAGIC release_date TIMESTAMP, 
# MAGIC total_tracks LONG, 
# MAGIC source_system STRING, 
# MAGIC ingestion_date TIMESTAMP
# MAGIC )
# MAGIC USING PARQUET
# MAGIC LOCATION "/mnt/datamladls26/lake/raw/technology/spotify/web-api/albums/"
# MAGIC

# COMMAND ----------

