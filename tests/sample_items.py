"""A trimmed copy of what the Spotify playlist_tracks endpoint returns."""


def track_item(song_id="s1", album_id="a1", artists=("ar1",), release_date="2024-05-17", popularity=80):
    return {
        "added_at": "2025-07-01T10:00:00Z",
        "track": {
            "id": song_id,
            "name": f"Song {song_id}",
            "duration_ms": 200000,
            "popularity": popularity,
            "album": {"id": album_id, "name": f"Album {album_id}", "release_date": release_date, "total_tracks": 12},
            "artists": [{"id": a, "name": f"Artist {a}"} for a in artists],
        },
    }


ITEMS = [
    track_item("s1", "a1", ("ar1", "ar2")),
    track_item("s2", "a1", ("ar1",)),
    track_item("s3", "a2", ("ar3",), release_date="2019"),
    {"added_at": "2025-07-01T10:00:00Z", "track": None},  # removed / local track
]
