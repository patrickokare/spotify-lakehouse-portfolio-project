# Databricks notebook source
# MAGIC %md
# MAGIC # Data quality checks
# MAGIC Rules are in `src/spotify_lakehouse/quality_rules.py`. Fails the job and lists every problem.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../.."))

from spotify_lakehouse import quality, quality_rules

failures = []

for table, rules in quality_rules.RULES.items():
    try:
        quality.run_checks(spark.table(table), table, rules)
    except quality.DataQualityError as error:
        failures.append(str(error))

# Songs must point at albums and artists we have
for child_table, key, parent_table in quality_rules.RELATIONSHIPS:
    orphans = spark.table(child_table).join(spark.table(parent_table).select(key), key, "left_anti").count()
    if orphans:
        failures.append(f"{child_table}.{key}: {orphans} rows with no match in {parent_table}")

if failures:
    raise quality.DataQualityError("\n\n".join(failures))

print("All data quality checks passed")
