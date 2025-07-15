# Databricks notebook source
# ------------------------------------------------------------------------------------
# Title: Create Externally Managed Delta Tables for Spotify Project
# Description: Initializes databases and creates raw tables in specific ADLS locations.
# Author: Patrick Okare
# Date: 2025-07-15
# ------------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Create Externally Managed Databases

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS spotify_raw
# MAGIC LOCATION '/mnt/datalakename/lake/raw/technology/spotify/web-api/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS spotify_cleansed
# MAGIC LOCATION '/mnt/datalakename/lake/cleansed/technology/spotify/web-api/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS spotify_modelled
# MAGIC LOCATION '/mnt/datalakename/lake/modelled/technology/spotify/web-api/'

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎵 Create Externally Managed Table: `raw_ext_albums`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS spotify_raw.raw_ext_albums
# MAGIC (
# MAGIC   album_id STRING,
# MAGIC   name STRING,
# MAGIC   release_date TIMESTAMP,
# MAGIC   total_tracks LONG,
# MAGIC   source_system STRING,
# MAGIC   ingestion_date TIMESTAMP
# MAGIC )
# MAGIC USING PARQUET
# MAGIC LOCATION '/mnt/datalakename/lake/raw/technology/spotify/web-api/albums/'
