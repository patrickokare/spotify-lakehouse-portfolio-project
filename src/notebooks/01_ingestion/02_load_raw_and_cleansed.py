# Databricks notebook source
# MAGIC %md
# MAGIC # Load: landing -> raw -> cleansed
# MAGIC For each entity (albums, artists, songs):
# MAGIC 1. Parse the landed track items (`src/spotify_lakehouse/parsing.py`).
# MAGIC 2. Append to the raw table (full history of every extract).
# MAGIC 3. Check the batch, then MERGE into cleansed (SCD Type 1 - only changed rows update).

# COMMAND ----------

import json
import os
import sys

sys.path.append(os.path.abspath("../.."))

from spotify_lakehouse import config, parsing, quality, quality_rules, transforms

# Set by the extract task when run as a job; pass it manually for an ad-hoc run
dbutils.widgets.text("landing_file", "")
landing_file = dbutils.widgets.get("landing_file") or dbutils.jobs.taskValues.get(
    taskKey="extract_playlist", key="landing_file", debugValue=""
)
if not landing_file:
    raise ValueError("No landing_file given")

# COMMAND ----------

# A playlist is at most a few thousand tracks, so reading it on the driver is fine
lines = spark.read.text(landing_file).collect()
items = [json.loads(line.value) for line in lines]
file_name = os.path.basename(landing_file)
print(f"{len(items)} items in {file_name}")

# COMMAND ----------

for entity, parse in parsing.PARSERS.items():
    rows = parse(items)
    batch = transforms.to_dataframe(spark, entity, rows)
    batch = transforms.add_audit_columns(batch, entity, config.SOURCE_SYSTEM, file_name)

    raw_table = config.RAW_TABLES[entity]
    cleansed_table = config.CLEANSED_TABLES[entity]
    key = config.KEYS[entity]

    # 1. raw: append
    batch.write.format("delta").mode("append").saveAsTable(raw_table)

    # 2. check before touching cleansed
    quality.run_checks(batch, f"{entity} batch", quality_rules.RULES[cleansed_table])

    # 3. cleansed: MERGE
    batch.createOrReplaceTempView("vw_batch")
    update_columns = [c for c in batch.columns if c not in (key, "ingestion_date")]
    set_clause = ", ".join(f"t.{c} = s.{c}" for c in update_columns)
    insert_columns = ", ".join(batch.columns)
    insert_values = ", ".join(f"s.{c}" for c in batch.columns)

    spark.sql(f"""
        MERGE INTO {cleansed_table} t
        USING vw_batch s
          ON t.{key} = s.{key}
        WHEN MATCHED AND t.HASH_ID <> s.HASH_ID THEN
          UPDATE SET {set_clause}, t.updated_date = current_timestamp()
        WHEN NOT MATCHED THEN
          INSERT ({insert_columns}) VALUES ({insert_values})
    """)
    print(f"{entity}: {len(rows)} rows -> {raw_table}, merged into {cleansed_table}")
