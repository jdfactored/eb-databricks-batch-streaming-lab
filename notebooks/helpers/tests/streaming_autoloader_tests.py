"""
Tests for notebook 02-streaming-autoloader.

Validates the bronze Delta table produced by Auto Loader streaming ingestion,
including schema, data types, data integrity, and schema evolution.

All tests are self-contained: they resolve the output table via the shared
helpers so no DataFrame or flags need to be passed in from the notebook.
"""

from pyspark.sql import SparkSession
from helpers import utils


def _get_table_and_spark():
    """Return (spark, fully-qualified table name) using the same logic as the notebook."""
    spark = SparkSession.builder.getOrCreate()
    catalog = utils.get_param("catalog", "capstone_dev")
    bronze = f"{utils.get_base_user_schema()}_bronze"
    return spark, f"{catalog}.{bronze}.web_site_events"


# ── existence & row count ────────────────────────────────────────────────

def test_table_exists(**kwargs):
    """The output table must exist after the streaming jobs run."""
    _, table = _get_table_and_spark()
    assert utils.table_exists(table), f"Table {table} does not exist"


def test_validate_row_count(**kwargs):
    """The table must contain rows from both the initial and evolved batches."""
    spark, table = _get_table_and_spark()
    row_count = spark.table(table).count()
    assert row_count > 0, f"Expected rows > 0, found {row_count}"


# ── schema & types ───────────────────────────────────────────────────────

def test_validate_schema(**kwargs):
    """All expected columns — including referrer_url from schema evolution — must be present."""
    spark, table = _get_table_and_spark()
    actual_columns = set(spark.table(table).columns)

    expected_columns = {
        "device_type", "endpoint", "event_id", "event_timestamp",
        "event_type", "product_id", "session_id", "user_id",
        "_rescued_data", "referrer_url",
    }
    missing = expected_columns - actual_columns
    assert not missing, f"Missing columns: {missing}"


def test_validate_column_types(**kwargs):
    """Key columns must carry the types Auto Loader inferred."""
    spark, table = _get_table_and_spark()
    schema_map = {f.name: f.dataType.simpleString() for f in spark.table(table).schema.fields}

    expected_types = {
        "event_id": "string",
        "session_id": "string",
        "user_id": "bigint",
        "product_id": "bigint",
        "referrer_url": "string",
    }
    for col, expected in expected_types.items():
        actual = schema_map.get(col)
        assert actual is not None, f"Column '{col}' not found in schema"
        assert actual == expected, (
            f"Column '{col}' has type '{actual}', expected '{expected}'"
        )


# ── data integrity ───────────────────────────────────────────────────────

def test_validate_data_integrity(**kwargs):
    """Critical identifier columns must have no nulls."""
    spark, table = _get_table_and_spark()
    df = spark.table(table)

    for col in ["event_id", "session_id"]:
        null_count = df.filter(df[col].isNull()).count()
        assert null_count == 0, f"Found {null_count} null values in '{col}'"


def test_validate_event_id_uniqueness(**kwargs):
    """Every event_id must be unique — checkpoint guarantees no duplicates."""
    spark, table = _get_table_and_spark()
    df = spark.table(table)

    total = df.count()
    distinct = df.select("event_id").distinct().count()
    assert distinct == total, (
        f"event_id not unique: {distinct} distinct vs {total} total rows"
    )


# ── schema evolution ─────────────────────────────────────────────────────

def test_schema_evolution_applied(**kwargs):
    """After evolution, some rows must have a non-null referrer_url."""
    spark, table = _get_table_and_spark()
    evolved_rows = spark.sql(
        f"SELECT COUNT(*) AS c FROM {table} WHERE referrer_url IS NOT NULL"
    ).collect()[0]["c"]
    assert evolved_rows > 0, (
        "No rows with non-null referrer_url — schema evolution may not have completed"
    )


def test_rescued_data_mostly_clean(**kwargs):
    """The vast majority of rows should parse cleanly (_rescued_data is null)."""
    spark, table = _get_table_and_spark()
    df = spark.table(table)

    total = df.count()
    rescued = df.filter(df["_rescued_data"].isNotNull()).count()
    ratio = rescued / total if total > 0 else 0
    assert ratio < 0.10, (
        f"{rescued}/{total} rows ({ratio:.1%}) have rescued data — expected < 10%"
    )

