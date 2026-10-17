"""Small data quality checks used by the pipeline and the DQ notebook.

Each check returns a list of failure messages (empty list = passed).
run_checks() raises once with every failure, so one run shows all problems.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityError(Exception):
    pass


def check_not_null(df: DataFrame, columns: list[str]) -> list[str]:
    counts = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columns]).first()
    return [f"{c}: {counts[c]} null values" for c in columns if counts[c]]


def check_unique(df: DataFrame, columns: list[str]) -> list[str]:
    duplicates = df.groupBy(columns).count().filter("count > 1").count()
    if duplicates:
        return [f"{', '.join(columns)}: {duplicates} duplicated keys"]
    return []


def check_accepted_values(df: DataFrame, column: str, allowed: list[str]) -> list[str]:
    bad = df.filter(F.col(column).isNotNull() & ~F.col(column).isin(allowed)).count()
    return [f"{column}: {bad} values outside {allowed}"] if bad else []


def check_range(df: DataFrame, column: str, min_value=None, max_value=None) -> list[str]:
    condition = F.lit(False)
    if min_value is not None:
        condition = condition | (F.col(column) < min_value)
    if max_value is not None:
        condition = condition | (F.col(column) > max_value)
    bad = df.filter(condition).count()
    return [f"{column}: {bad} values outside [{min_value}, {max_value}]"] if bad else []


def check_unknown_key_rate(df: DataFrame, key_columns: list[str], max_pct: float) -> list[str]:
    """Fact rows pointing at the -1 'Unknown' member, as a % of all rows."""
    total = df.count()
    if total == 0:
        return []
    failures = []
    for key in key_columns:
        unknown = df.filter(F.col(key) == -1).count()
        pct = unknown / total * 100
        if pct > max_pct:
            failures.append(f"{key}: {pct:.2f}% unknown members (limit {max_pct}%)")
    return failures


def run_checks(df: DataFrame, table_name: str, rules: dict) -> None:
    """Run the rules for one table. Raises DataQualityError if anything fails.

    rules example:
        {"not_null": ["DOLVehicleID"],
         "unique": [["DOLVehicleID"]],
         "accepted_values": {"State": ["WA"]},
         "range": {"ModelYear": (1990, 2030)}}
    """
    failures = []
    if rules.get("not_null"):
        failures += check_not_null(df, rules["not_null"])
    for key in rules.get("unique", []):
        failures += check_unique(df, key)
    for column, allowed in rules.get("accepted_values", {}).items():
        failures += check_accepted_values(df, column, allowed)
    for column, (low, high) in rules.get("range", {}).items():
        failures += check_range(df, column, low, high)

    if failures:
        message = "\n  - ".join([f"DQ failed for {table_name}:"] + failures)
        raise DataQualityError(message)
    print(f"[DQ] {table_name}: all checks passed")
