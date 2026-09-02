"""Checks for B03-performance-tuning."""

from helpers import utils

spark = utils.spark


def test_wide_events_table_exists(**kwargs):
    cfg = utils.get_configs("events_wide")
    assert utils.table_exists(cfg["table_silver"]), (
        f"{cfg['table_silver']} does not exist. Step 1 builds the table you will tune."
    )


def test_table_is_large_enough_to_matter(**kwargs):
    cfg = utils.get_configs("events_wide")
    rows = utils.get_table_row_count(cfg["table_silver"])
    assert rows >= 100_000, (
        f"{cfg['table_silver']} has only {rows:,} rows. Step 1 asks you to amplify the event "
        f"data — tuning measurements on a tiny table are noise."
    )


def test_baseline_measurements_recorded(**kwargs):
    cfg = utils.get_configs("tuning_log")
    assert utils.table_exists(cfg["table_gold"]), (
        f"{cfg['table_gold']} does not exist. Step 2 asks you to record every measurement so "
        f"the before/after comparison is evidence rather than a feeling."
    )
    cols = set(utils.get_column_names(cfg["table_gold"]))
    required = {"stage", "num_files", "size_bytes", "files_scanned", "duration_ms"}
    missing = required - cols
    assert not missing, f"tuning_log is missing {sorted(missing)}."


def test_measured_before_and_after(**kwargs):
    cfg = utils.get_configs("tuning_log")
    stages = {
        row["stage"]
        for row in spark.sql(f"SELECT DISTINCT stage FROM {cfg['table_gold']}").collect()
    }
    assert {"before", "after"} <= stages, (
        f"tuning_log contains stages {sorted(stages)}. You need at least a 'before' and an "
        f"'after' row to make any claim about the effect of your changes."
    )


def test_clustering_enabled(**kwargs):
    cfg = utils.get_configs("events_wide")
    clustering = utils.get_detail(cfg["table_silver"]).get("clusteringColumns") or []
    assert clustering, (
        f"{cfg['table_silver']} has no clustering columns. Step 3 asks you to pick keys from "
        f"the query predicates and enable liquid clustering."
    )


def test_optimize_ran(**kwargs):
    cfg = utils.get_configs("events_wide")
    ops = utils.get_history_operations(cfg["table_silver"])
    assert "OPTIMIZE" in ops, (
        f"No OPTIMIZE in the history of {cfg['table_silver']}. Enabling clustering does not "
        f"rewrite existing data — OPTIMIZE is what applies the layout."
    )


def test_file_count_improved(**kwargs):
    cfg = utils.get_configs("tuning_log")
    rows = spark.sql(f"""
        SELECT stage, MIN(num_files) AS files
        FROM {cfg['table_gold']}
        WHERE stage IN ('before', 'after')
        GROUP BY stage
    """).collect()
    by_stage = {r["stage"]: r["files"] for r in rows}
    assert by_stage.get("after", 1e9) <= by_stage.get("before", 0), (
        f"File count went from {by_stage.get('before')} to {by_stage.get('after')}. "
        f"Compaction should not increase the number of files — check the order of your steps."
    )


def test_files_skipped_after_clustering(**kwargs):
    """The measurement that actually matters: fewer files touched by the same query."""
    cfg = utils.get_configs("tuning_log")
    rows = spark.sql(f"""
        SELECT stage, MIN(files_scanned) AS scanned
        FROM {cfg['table_gold']}
        WHERE stage IN ('before', 'after')
        GROUP BY stage
    """).collect()
    by_stage = {r["stage"]: r["scanned"] for r in rows}
    before, after = by_stage.get("before"), by_stage.get("after")
    assert before is not None and after is not None, "Missing files_scanned for one stage."
    assert after < before, (
        f"The tuned query still scans {after} file(s) versus {before} before. Clustering on a "
        f"column the query does not filter on skips nothing — revisit your key choice."
    )
