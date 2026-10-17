# Runbook

## Setup

1. Create a Spotify app at <https://developer.spotify.com/dashboard> and store its credentials in Key Vault.
2. Create a Key Vault-backed secret scope called `spotify-keyvault` with the keys `Spotify-Client-id` and `Spotify-Client-Secret`.
3. Deploy and create the tables:

```bash
pip install -r requirements-dev.txt && pytest
databricks bundle deploy -t dev
databricks bundle run spotify_setup -t dev
databricks bundle run spotify_daily_load -t dev
```

To track a different playlist: `databricks bundle run spotify_daily_load -t dev --params playlist_id=<id>`.

> Spotify limits API access to some editorial playlists for newer apps. If the extract fails with a 404, use a public user playlist id.

## Failures

| Failure | Meaning | What to do |
|---|---|---|
| Extract: `returned no tracks` / 401 / 404 | Bad playlist id or credentials | Check the secret scope and playlist id. The task retries twice for network blips. |
| `DQ failed for <entity> batch` | The API returned bad data | Nothing reached cleansed. The landing file is kept for investigation. |
| Orphan `album_id` / `artist_id` | A song refers to something that wasn't loaded | Replay the landing file (below) |

## Re-processing a landing file

Landing files are never deleted, so any run can be replayed. Open `01_ingestion/02_load_raw_and_cleansed`,
set the `landing_file` widget to the file path, and run it; then run `02_modelled` and `03_quality`.
The cleansed MERGE is idempotent, so replaying the same file twice changes nothing.
