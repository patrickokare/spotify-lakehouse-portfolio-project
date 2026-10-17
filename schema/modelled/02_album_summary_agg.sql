-- Albums in the playlist: size of the album and how many of its songs made the playlist
CREATE OR REPLACE TABLE spotify_modelled.AlbumSummaryAgg AS
SELECT
    a.album_id AS AlbumID,
    a.album_name AS AlbumName,
    a.release_date AS ReleaseDate,
    a.total_tracks AS TotalTracks,
    COUNT(s.song_id) AS SongsInPlaylist,
    ROUND(AVG(s.popularity), 1) AS AvgPopularity
FROM spotify_cleansed.albums a
JOIN spotify_cleansed.songs s ON s.album_id = a.album_id
GROUP BY a.album_id, a.album_name, a.release_date, a.total_tracks;
