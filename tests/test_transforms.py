from sample_items import ITEMS, track_item

from spotify_lakehouse import parsing, transforms


def test_dataframes_get_proper_types(spark):
    albums = transforms.to_dataframe(spark, "albums", parsing.parse_albums(ITEMS))
    songs = transforms.to_dataframe(spark, "songs", parsing.parse_songs(ITEMS))
    assert dict(albums.dtypes)["release_date"] == "date"
    assert dict(songs.dtypes)["song_added"] == "timestamp"
    assert songs.filter("song_added IS NULL").count() == 0


def test_audit_columns(spark):
    df = transforms.to_dataframe(spark, "artists", parsing.parse_artists(ITEMS))
    df = transforms.add_audit_columns(df, "artists", "spotify-web-api", "playlist_x.json")
    row = df.first()
    assert row.source_system == "spotify-web-api"
    assert row.source_file == "playlist_x.json"
    assert len(row.HASH_ID) == 64


def song_hash(spark, item):
    df = transforms.to_dataframe(spark, "songs", parsing.parse_songs([item]))
    return transforms.add_audit_columns(df, "songs", "x", "f").first().HASH_ID


def test_hash_is_stable_across_runs(spark):
    """Same data on a later run must give the same hash, otherwise MERGE rewrites every row."""
    assert song_hash(spark, track_item()) == song_hash(spark, track_item())


def test_hash_changes_when_popularity_changes(spark):
    assert song_hash(spark, track_item(popularity=80)) != song_hash(spark, track_item(popularity=81))
