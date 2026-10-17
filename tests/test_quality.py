import pytest

from spotify_lakehouse import quality


@pytest.fixture
def vehicles(spark):
    return spark.createDataFrame(
        [(1, "TESLA", 2020, -1), (2, None, 2021, 10), (2, "KIA", 1800, 11)],
        "id int, make string, year int, location_key int",
    )


def test_not_null(vehicles):
    assert quality.check_not_null(vehicles, ["id"]) == []
    assert quality.check_not_null(vehicles, ["make"]) == ["make: 1 null values"]


def test_unique(vehicles):
    assert quality.check_unique(vehicles, ["id"]) == ["id: 1 duplicated keys"]
    assert quality.check_unique(vehicles, ["id", "year"]) == []


def test_accepted_values_ignores_nulls(vehicles):
    assert quality.check_accepted_values(vehicles, "make", ["TESLA", "KIA"]) == []
    assert len(quality.check_accepted_values(vehicles, "make", ["TESLA"])) == 1


def test_range(vehicles):
    assert quality.check_range(vehicles, "year", 1990, 2035) == ["year: 1 values outside [1990, 2035]"]
    assert quality.check_range(vehicles, "year", min_value=1700) == []


def test_unknown_key_rate(vehicles):
    assert quality.check_unknown_key_rate(vehicles, ["location_key"], max_pct=50) == []
    assert len(quality.check_unknown_key_rate(vehicles, ["location_key"], max_pct=10)) == 1


def test_run_checks_reports_every_failure_at_once(vehicles):
    rules = {"not_null": ["make"], "unique": [["id"]], "range": {"year": (1990, 2035)}}
    with pytest.raises(quality.DataQualityError) as error:
        quality.run_checks(vehicles, "vehicles", rules)
    message = str(error.value)
    assert "make: 1 null values" in message
    assert "id: 1 duplicated keys" in message
    assert "year: 1 values outside" in message


def test_run_checks_passes_clean_data(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], "id int, name string")
    quality.run_checks(df, "clean", {"not_null": ["id", "name"], "unique": [["id"]]})
