"""Turn Spotify playlist track items into flat album / artist / song rows.

Plain Python (no Spark) so it is quick to test. Spotify returns one "item" per
playlist entry; item["track"] can be None for removed or local tracks.
"""


def normalize_release_date(value: str | None) -> str | None:
    """Spotify release dates come as '2024', '2024-05' or '2024-05-17'. Always return a full date."""
    if not value:
        return None
    parts = value.split("-")
    while len(parts) < 3:
        parts.append("01")
    return "-".join(parts[:3])


def _tracks(items: list[dict]):
    for item in items:
        track = item.get("track")
        if track and track.get("id"):
            yield item, track


def parse_albums(items: list[dict]) -> list[dict]:
    albums = {}
    for _, track in _tracks(items):
        album = track["album"]
        albums[album["id"]] = {
            "album_id": album["id"],
            "album_name": album["name"],
            "release_date": normalize_release_date(album.get("release_date")),
            "total_tracks": album.get("total_tracks"),
        }
    return list(albums.values())


def parse_artists(items: list[dict]) -> list[dict]:
    artists = {}
    for _, track in _tracks(items):
        for artist in track["artists"]:
            artists[artist["id"]] = {"artist_id": artist["id"], "artist_name": artist["name"]}
    return list(artists.values())


def parse_songs(items: list[dict]) -> list[dict]:
    songs = {}
    for item, track in _tracks(items):
        songs[track["id"]] = {
            "song_id": track["id"],
            "song_name": track["name"],
            "duration_ms": track.get("duration_ms"),
            "popularity": track.get("popularity"),
            "song_added": item.get("added_at"),
            "album_id": track["album"]["id"],
            # first listed artist is the primary artist of the track
            "artist_id": track["artists"][0]["id"] if track["artists"] else None,
        }
    return list(songs.values())


PARSERS = {"albums": parse_albums, "artists": parse_artists, "songs": parse_songs}
