-- Raw: every load is appended, so we keep the history of what the API returned.
CREATE TABLE IF NOT EXISTS spotify_raw.raw_ext_albums (
    album_id       STRING,
    album_name     STRING,
    release_date   DATE,
    total_tracks   INT,
    source_system  STRING,
    source_file    STRING,
    ingestion_date TIMESTAMP,
    HASH_ID        STRING
)
USING DELTA
LOCATION '{lake_root}/lake/raw/technology/spotify/web-api/albums';
