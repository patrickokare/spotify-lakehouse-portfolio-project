from sample_items import ITEMS

from spotify_lakehouse import parsing


def test_release_date_is_always_a_full_date():
    assert parsing.normalize_release_date("2024-05-17") == "2024-05-17"
    assert parsing.normalize_release_date("2024-05") == "2024-05-01"
    assert parsing.normalize_release_date("2019") == "2019-01-01"
    assert parsing.normalize_release_date(None) is None


def test_albums_are_unique_and_skip_missing_tracks():
    albums = parsing.parse_albums(ITEMS)
    assert sorted(a["album_id"] for a in albums) == ["a1", "a2"]
    assert {a["album_id"]: a["release_date"] for a in albums}["a2"] == "2019-01-01"


def test_every_artist_on_a_track_is_captured():
    artists = parsing.parse_artists(ITEMS)
    assert sorted(a["artist_id"] for a in artists) == ["ar1", "ar2", "ar3"]


def test_song_uses_first_track_artist_as_primary():
    songs = {s["song_id"]: s for s in parsing.parse_songs(ITEMS)}
    assert set(songs) == {"s1", "s2", "s3"}
    assert songs["s1"]["artist_id"] == "ar1"
    assert songs["s1"]["song_added"] == "2025-07-01T10:00:00Z"


def test_empty_playlist():
    assert parsing.parse_songs([]) == []
