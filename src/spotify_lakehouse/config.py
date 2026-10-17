"""Names, paths and source settings for the Spotify lakehouse."""

SOURCE_SYSTEM = "spotify-web-api"
PROJECT_PATH = "technology/spotify/web-api"

# Spotify "Top 50 - Global". Any public playlist id works.
DEFAULT_PLAYLIST_ID = "37i9dQZEVXbNG2KDcFcKOF"

# Databricks secret scope (backed by Azure Key Vault) and key names
SECRET_SCOPE = "spotify-keyvault"
CLIENT_ID_KEY = "Spotify-Client-id"
CLIENT_SECRET_KEY = "Spotify-Client-Secret"

RAW_DB = "spotify_raw"
CLEANSED_DB = "spotify_cleansed"
MODELLED_DB = "spotify_modelled"

RAW_TABLES = {
    "albums": f"{RAW_DB}.raw_ext_albums",
    "artists": f"{RAW_DB}.raw_ext_artists",
    "songs": f"{RAW_DB}.raw_ext_songs",
}

CLEANSED_TABLES = {
    "albums": f"{CLEANSED_DB}.albums",
    "artists": f"{CLEANSED_DB}.artists",
    "songs": f"{CLEANSED_DB}.songs",
}

# Business key of each entity
KEYS = {"albums": "album_id", "artists": "artist_id", "songs": "song_id"}


def landing_path(lake_root: str) -> str:
    return f"{lake_root}/landing/databricks/{PROJECT_PATH}/playlist-tracks"
