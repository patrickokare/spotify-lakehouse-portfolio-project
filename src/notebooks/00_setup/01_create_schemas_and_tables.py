# Databricks notebook source
# MAGIC %md
# MAGIC # Create schemas and tables
# MAGIC DDL lives in `/schema/raw` and `/schema/cleansed`. Safe to re-run.

# COMMAND ----------

import glob
import os

dbutils.widgets.text("lake_root", "/mnt/datalake")
lake_root = dbutils.widgets.get("lake_root")

schema_dir = os.path.abspath("../../../schema")

# COMMAND ----------

for database, layer in [("spotify_raw", "raw"), ("spotify_cleansed", "cleansed"), ("spotify_modelled", "modelled")]:
    location = f"{lake_root}/lake/{layer}/technology/spotify/web-api/"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database} LOCATION '{location}'")

for layer in ["raw", "cleansed"]:
    for path in sorted(glob.glob(f"{schema_dir}/{layer}/*.sql")):
        with open(path) as f:
            spark.sql(f.read().replace("{lake_root}", lake_root))
        print(f"Applied {layer}/{os.path.basename(path)}")
