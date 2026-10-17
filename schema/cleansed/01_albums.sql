-- Cleansed: one current row per album (SCD Type 1, merged on album_id)
CREATE TABLE IF NOT EXISTS spotify_cleansed.albums (
    album_id       STRING NOT NULL,
    album_name     STRING,
    release_date   DATE,
    total_tracks   INT,
    source_system  STRING,
    source_file    STRING,
    ingestion_date TIMESTAMP,
    updated_date   TIMESTAMP,
    HASH_ID        STRING
)
USING DELTA
LOCATION '{lake_root}/lake/cleansed/technology/spotify/web-api/albums';
