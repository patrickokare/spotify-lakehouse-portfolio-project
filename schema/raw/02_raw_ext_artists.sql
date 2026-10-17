CREATE TABLE IF NOT EXISTS spotify_raw.raw_ext_artists (
    artist_id      STRING,
    artist_name    STRING,
    source_system  STRING,
    source_file    STRING,
    ingestion_date TIMESTAMP,
    HASH_ID        STRING
)
USING DELTA
LOCATION '{lake_root}/lake/raw/technology/spotify/web-api/artists';
