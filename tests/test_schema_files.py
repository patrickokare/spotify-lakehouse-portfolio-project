from pathlib import Path

import pytest

from spotify_lakehouse import transforms

SCHEMA_DIR = Path(__file__).parent.parent / "schema"


def statements(path):
    code = "\n".join(line for line in path.read_text().splitlines() if not line.strip().startswith("--"))
    return [s for s in code.split(";") if s.strip()]


@pytest.mark.parametrize("path", sorted(SCHEMA_DIR.rglob("*.sql")), ids=lambda p: p.name)
def test_one_statement_per_file(path):
    assert len(statements(path)) == 1


@pytest.mark.parametrize("entity", ["albums", "artists", "songs"])
def test_cleansed_table_has_every_batch_column(entity):
    """The MERGE inserts every batch column, so the table must have all of them."""
    ddl = next((SCHEMA_DIR / "cleansed").glob(f"*_{entity}.sql")).read_text()
    batch_columns = [c.split()[0] for c in transforms.SCHEMAS[entity].split(", ")]
    batch_columns += ["source_system", "source_file", "ingestion_date", "HASH_ID"]
    for column in batch_columns:
        assert f"    {column} " in ddl, column
