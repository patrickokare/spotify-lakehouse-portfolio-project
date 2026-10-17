CREATE TABLE IF NOT EXISTS spotify_cleansed.artists (
    artist_id      STRING NOT NULL,
    artist_name    STRING,
    source_system  STRING,
    source_file    STRING,
    ingestion_date TIMESTAMP,
    updated_date   TIMESTAMP,
    HASH_ID        STRING
)
USING DELTA
LOCATION '{lake_root}/lake/cleansed/technology/spotify/web-api/artists';
