"""
Checks for S02-ingesting-streams-autoloader.

These are exact-count assertions, which is only possible because the replay files are
generated deterministically. Expected values come from the manifest, never hardcoded.
"""

from helpers import utils, event_stream

spark = utils.spark


def _manifest():
    return event_stream.read_manifest()


def test_replay_files_present(**kwargs):
    path = event_stream.replay_path()
    files = [f for f in utils.list_files(path) if f.endswith(".json")]
    expected = _manifest()["files"]
    assert len(files) == expected, (
        f"Found {len(files)} replay file(s) at {path}, expected {expected}. Re-run 00-setup."
    )


def test_bronze_events_exists(**kwargs):
    cfg = utils.get_configs("web_events")
    assert utils.table_exists(cfg["table_bronze"]), (
        f"{cfg['table_bronze']} does not exist. Step 2 lands the replay stream in bronze."
    )


def test_all_events_ingested(**kwargs):
    cfg = utils.get_configs("web_events")
    manifest = _manifest()
    rows = utils.get_table_row_count(cfg["table_bronze"])
    assert rows == manifest["total_events"], (
        f"Bronze has {rows:,} rows, expected exactly {manifest['total_events']:,}. "
        f"Fewer means the stream stopped early; more means it consumed a file twice."
    )


def test_one_micro_batch_per_file(**kwargs):
    """
    `maxFilesPerTrigger = 1` is what makes this exercise reproducible: one commit per file.
    """
    cfg = utils.get_configs("web_events")
    manifest = _manifest()
    commits = [
        op for op in utils.get_history_operations(cfg["table_bronze"])
        if "STREAMING" in op.upper()
    ]
    assert len(commits) == manifest["files"], (
        f"{len(commits)} streaming commit(s) for {manifest['files']} files. Step 2 asks for "
        f"`cloudFiles.maxFilesPerTrigger = 1` so each file becomes its own micro-batch."
    )


def test_rescued_data_column_present(**kwargs):
    cfg = utils.get_configs("web_events")
    cols = utils.get_column_names(cfg["table_bronze"])
    assert any("rescued" in c.lower() for c in cols), (
        f"No rescued-data column in bronze. Without it, a value that does not fit the inferred "
        f"schema is silently discarded. Columns: {cols}"
    )


def test_schema_evolved_to_include_referrer(**kwargs):
    cfg = utils.get_configs("web_events")
    cols = utils.get_column_names(cfg["table_bronze"])
    assert "referrer_url" in cols, (
        f"`referrer_url` is missing. It first appears in file "
        f"{_manifest()['schema_drift_file']:03d}; Auto Loader stops the stream when it sees a "
        f"new column, and Step 3 asks you to restart so the column is adopted. Columns: {cols}"
    )


def test_referrer_populated_only_for_later_files(**kwargs):
    cfg = utils.get_configs("web_events")
    manifest = _manifest()
    with_referrer = spark.sql(
        f"SELECT COUNT(*) AS c FROM {cfg['table_bronze']} WHERE referrer_url IS NOT NULL"
    ).collect()[0]["c"]
    assert with_referrer > 0, (
        "`referrer_url` exists but is null in every row — the column was added to the table "
        "but the later files were never reprocessed after the restart."
    )
    total = manifest["total_events"]
    assert with_referrer < total, (
        f"Every row has a referrer_url ({with_referrer:,}/{total:,}), which cannot be right: "
        f"files before {manifest['schema_drift_file']:03d} do not carry the field."
    )


def test_schema_location_created(**kwargs):
    cfg = utils.get_configs("web_events")
    assert utils.path_exists(cfg["schema_path"]), (
        f"No Auto Loader schema location at {cfg['schema_path']}. `cloudFiles.schemaLocation` "
        f"is required, and its versioned contents are how you audit a schema change."
    )


def test_silver_deduplicated(**kwargs):
    cfg = utils.get_configs("web_events")
    manifest = _manifest()
    assert utils.table_exists(cfg["table_silver"]), (
        f"{cfg['table_silver']} does not exist. See Step 4."
    )
    rows = utils.get_table_row_count(cfg["table_silver"])
    assert rows == manifest["distinct_event_ids"], (
        f"Silver has {rows:,} rows, expected {manifest['distinct_event_ids']:,} distinct "
        f"event_ids. Bronze legitimately contains {manifest['duplicate_events']} duplicate(s) "
        f"because each file replays a few events from the previous one."
    )
