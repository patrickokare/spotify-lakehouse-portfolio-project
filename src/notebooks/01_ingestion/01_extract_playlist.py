# Databricks notebook source
# MAGIC %md
# MAGIC # Extract: Spotify playlist -> landing
# MAGIC Calls the Spotify Web API once and lands the raw track items as JSON lines.
# MAGIC Credentials come from a Key Vault-backed secret scope - nothing is stored in code.

# COMMAND ----------

# MAGIC %pip install spotipy==2.24.0

# COMMAND ----------

import json
import os
import sys
from datetime import datetime, timezone

sys.path.append(os.path.abspath("../.."))

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from spotify_lakehouse import config

dbutils.widgets.text("lake_root", "/mnt/datalake")
dbutils.widgets.text("playlist_id", config.DEFAULT_PLAYLIST_ID)
lake_root = dbutils.widgets.get("lake_root")
playlist_id = dbutils.widgets.get("playlist_id")

# COMMAND ----------

credentials = SpotifyClientCredentials(
    client_id=dbutils.secrets.get(config.SECRET_SCOPE, config.CLIENT_ID_KEY),
    client_secret=dbutils.secrets.get(config.SECRET_SCOPE, config.CLIENT_SECRET_KEY),
)
sp = spotipy.Spotify(client_credentials_manager=credentials, retries=3)

# The API returns 100 tracks per page - follow "next" until the end
page = sp.playlist_tracks(playlist_id)
items = page["items"]
while page["next"]:
    page = sp.next(page)
    items.extend(page["items"])

print(f"Fetched {len(items)} playlist items")
if not items:
    raise ValueError(f"Playlist {playlist_id} returned no tracks - check the id and API access")

# COMMAND ----------

run_time = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
file_path = f"{config.landing_path(lake_root)}/playlist_{playlist_id}_{run_time}.json"

dbutils.fs.put(file_path, "\n".join(json.dumps(item) for item in items), overwrite=False)

# Hand the file name to the next task in the job
dbutils.jobs.taskValues.set(key="landing_file", value=file_path)
print(f"Landed {file_path}")
