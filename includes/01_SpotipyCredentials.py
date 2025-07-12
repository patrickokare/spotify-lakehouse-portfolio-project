# Databricks notebook source
pip install spotipy

# COMMAND ----------

!pip install pytz

# COMMAND ----------

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

# COMMAND ----------

  # Get secrets from Key Vault
Spotify_Client_id= dbutils.secrets.get(scope = 'somerandom-keyvault', key = 'Spotify-Client-id')
Spotify_Client_Secrets = dbutils.secrets.get(scope = 'somerandom-keyvault', key = 'Spotify-Client-Secret')

# COMMAND ----------

client_credentials_manager = SpotifyClientCredentials(client_id= Spotify_Client_id, client_secret= Spotify_Client_Secrets)

# COMMAND ----------

# client_credentials_manager = SpotifyClientCredentials(client_id="redactedxxxx", client_secret="redactedxxxxx")

# COMMAND ----------

# current_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# print(current_path)