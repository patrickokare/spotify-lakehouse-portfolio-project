"""Spark helpers shared by the ingestion notebooks."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SCHEMAS = {
    "albums": "album_id string, album_name string, release_date string, total_tracks int",
    "artists": "artist_id string, artist_name string",
    "songs": (
        "song_id string, song_name string, duration_ms long, popularity int, "
        "song_added string, album_id string, artist_id string"
    ),
}

# Columns used for change detection. Load metadata is excluded on purpose:
# including ingestion_date would make every row look changed on every run.
HASH_COLUMNS = {
    "albums": ["album_name", "release_date", "total_tracks"],
    "artists": ["artist_name"],
    "songs": ["song_name", "duration_ms", "popularity", "album_id", "artist_id"],
}


def to_dataframe(spark: SparkSession, entity: str, rows: list[dict]) -> DataFrame:
    """Rows from parsing.py -> typed DataFrame."""
    df = spark.createDataFrame(rows, SCHEMAS[entity])
    if entity == "albums":
        df = df.withColumn("release_date", F.to_date("release_date"))
    if entity == "songs":
        df = df.withColumn("song_added", F.to_timestamp("song_added"))
    return df


def row_hash(columns: list[str]) -> F.Column:
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns]
    return F.sha2(F.concat_ws("|", *parts), 256)


def add_audit_columns(df: DataFrame, entity: str, source_system: str, source_file: str) -> DataFrame:
    return (
        df.withColumn("source_system", F.lit(source_system))
        .withColumn("source_file", F.lit(source_file))
        .withColumn("ingestion_date", F.current_timestamp())
        .withColumn("HASH_ID", row_hash(HASH_COLUMNS[entity]))
    )
