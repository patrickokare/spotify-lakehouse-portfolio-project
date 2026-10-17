"""Data quality rules per table. Used by the ingestion and quality notebooks."""

from spotify_lakehouse import config

RULES = {
    config.CLEANSED_TABLES["albums"]: {
        "not_null": ["album_id", "album_name", "HASH_ID"],
        "unique": [["album_id"]],
        "range": {"total_tracks": (1, 1000)},
    },
    config.CLEANSED_TABLES["artists"]: {
        "not_null": ["artist_id", "artist_name"],
        "unique": [["artist_id"]],
    },
    config.CLEANSED_TABLES["songs"]: {
        "not_null": ["song_id", "song_name", "album_id", "artist_id"],
        "unique": [["song_id"]],
        "range": {"popularity": (0, 100), "duration_ms": (1, 3_600_000)},
    },
    f"{config.MODELLED_DB}.SongsPopularityAgg": {
        "not_null": ["SongID", "PopularityRank"],
        "unique": [["SongID"]],
    },
}

# Every song must point at an album and an artist we have loaded.
RELATIONSHIPS = [
    (config.CLEANSED_TABLES["songs"], "album_id", config.CLEANSED_TABLES["albums"]),
    (config.CLEANSED_TABLES["songs"], "artist_id", config.CLEANSED_TABLES["artists"]),
]
