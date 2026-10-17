-- Artists ranked by number of songs in the playlist
CREATE OR REPLACE TABLE spotify_modelled.ArtistSummaryAgg AS
SELECT
    ar.artist_id AS ArtistID,
    ar.artist_name AS ArtistName,
    COUNT(DISTINCT s.song_id) AS NumberOfSongs,
    COUNT(DISTINCT s.album_id) AS NumberOfAlbums,
    ROUND(AVG(s.popularity), 1) AS AvgPopularity,
    RANK() OVER (ORDER BY COUNT(DISTINCT s.song_id) DESC) AS ArtistRank
FROM spotify_cleansed.artists ar
JOIN spotify_cleansed.songs s ON s.artist_id = ar.artist_id
GROUP BY ar.artist_id, ar.artist_name;
