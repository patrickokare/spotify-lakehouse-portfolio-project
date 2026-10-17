CREATE TABLE IF NOT EXISTS spotify_raw.raw_ext_songs (
    song_id        STRING,
    song_name      STRING,
    duration_ms    BIGINT,
    popularity     INT,
    song_added     TIMESTAMP,
    album_id       STRING,
    artist_id      STRING,
    source_system  STRING,
    source_file    STRING,
    ingestion_date TIMESTAMP,
    HASH_ID        STRING
)
USING DELTA
LOCATION '{lake_root}/lake/raw/technology/spotify/web-api/songs';
