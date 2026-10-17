-- Songs ranked by current popularity (0-100 score from Spotify)
CREATE OR REPLACE TABLE spotify_modelled.SongsPopularityAgg AS
SELECT
    DENSE_RANK() OVER (ORDER BY s.popularity DESC) AS PopularityRank,
    s.song_id AS SongID,
    s.song_name AS SongName,
    ar.artist_name AS ArtistName,
    a.album_name AS AlbumName,
    s.popularity AS Popularity,
    ROUND(s.duration_ms / 60000.0, 2) AS DurationMinutes
FROM spotify_cleansed.songs s
JOIN spotify_cleansed.albums a ON a.album_id = s.album_id
JOIN spotify_cleansed.artists ar ON ar.artist_id = s.artist_id;
