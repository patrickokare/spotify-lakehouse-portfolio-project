# Databricks notebook source
# MAGIC %md
# MAGIC # Build reporting tables (spotify_modelled)
# MAGIC Small tables, so each is fully rebuilt from cleansed (`CREATE OR REPLACE TABLE ... AS SELECT`).
# MAGIC Queries live in `/schema/modelled`.

# COMMAND ----------

import glob
import os

schema_dir = os.path.abspath("../../../schema/modelled")

for path in sorted(glob.glob(f"{schema_dir}/*.sql")):
    with open(path) as f:
        spark.sql(f.read())
    print(f"Built {os.path.basename(path)}")

# COMMAND ----------

display(spark.table("spotify_modelled.SongsPopularityAgg").orderBy("PopularityRank").limit(20))
