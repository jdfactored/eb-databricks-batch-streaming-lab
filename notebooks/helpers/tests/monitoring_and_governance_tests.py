"""Checks for S04-monitoring-and-governance."""

from helpers import utils

spark = utils.spark


def test_metrics_table_exists(**kwargs):
    cfg = utils.get_configs("stream_metrics")
    assert utils.table_exists(cfg["table_gold"]), (
        f"{cfg['table_gold']} does not exist. Step 1 asks you to persist the per-batch progress "
        f"metrics — a metric you only ever print is a metric you cannot alert on."
    )


def test_metrics_capture_the_right_fields(**kwargs):
    cfg = utils.get_configs("stream_metrics")
    cols = set(utils.get_column_names(cfg["table_gold"]))
    required = {"batch_id", "input_rows", "rows_per_second", "batch_duration_ms"}
    missing = required - cols
    assert not missing, (
        f"stream_metrics is missing {sorted(missing)}. These are the four numbers that tell "
        f"you whether a stream is keeping up."
    )


def test_metrics_cover_several_batches(**kwargs):
    cfg = utils.get_configs("stream_metrics")
    batches = spark.sql(
        f"SELECT COUNT(DISTINCT batch_id) AS c FROM {cfg['table_gold']}"
    ).collect()[0]["c"]
    assert batches >= 3, (
        f"Only {batches} batch(es) recorded. A single data point cannot show a trend."
    )


def test_throughput_comparison_recorded(**kwargs):
    """Step 2 compares two maxFilesPerTrigger settings on the same data."""
    cfg = utils.get_configs("stream_tuning")
    assert utils.table_exists(cfg["table_gold"]), (
        f"{cfg['table_gold']} does not exist. See Step 2."
    )
    configs = spark.sql(
        f"SELECT COUNT(DISTINCT config_name) AS c FROM {cfg['table_gold']}"
    ).collect()[0]["c"]
    assert configs >= 2, (
        f"Only {configs} configuration(s) recorded. Step 2 asks you to run the same replay with "
        f"two different trigger settings and compare batch count against batch duration."
    )


def test_streaming_table_has_owner_comment(**kwargs):
    """Governance starts with the table being self-describing."""
    cfg = utils.get_configs("web_events")
    comment = utils.get_detail(cfg["table_silver"]).get("description")
    assert comment, (
        f"{cfg['table_silver']} has no table comment. Step 3 asks you to document every "
        f"streaming table — an undocumented table in a shared catalog is a support ticket."
    )


def test_pii_columns_tagged(**kwargs):
    cfg = utils.get_configs("events_enriched")
    tags = spark.sql(f"""
        SELECT column_name, tag_name
        FROM {utils.get_catalog()}.information_schema.column_tags
        WHERE schema_name = '{utils.get_configs()['schema_silver']}'
          AND table_name = 'events_enriched'
    """).collect()
    assert tags, (
        f"No column tags found on events_enriched. Step 3 asks you to tag the PII columns so "
        f"they can be discovered and governed centrally rather than by convention."
    )


def test_masked_stream_view_exists(**kwargs):
    cfg = utils.get_configs()
    view = f"{cfg['catalog']}.{cfg['schema_gold']}.v_events_masked"
    assert utils.table_exists(view), (
        f"{view} does not exist. Step 4 asks for a governed view over the streaming table."
    )


def test_masked_view_hides_pii(**kwargs):
    cfg = utils.get_configs()
    view = f"{cfg['catalog']}.{cfg['schema_gold']}.v_events_masked"
    sql = spark.sql(f"SHOW CREATE TABLE {view}").collect()[0][0].lower()
    assert "is_account_group_member" in sql or "is_member" in sql, (
        "The view has no group check, so it masks nothing for anyone. Use "
        "is_account_group_member() to branch on the caller's group membership."
    )


def test_no_streams_left_running(**kwargs):
    """Leaving a processingTime query active burns compute after the lab ends."""
    active = spark.streams.active
    assert not active, (
        f"{len(active)} streaming query/queries still active: "
        f"{[q.name or str(q.id) for q in active]}. Call "
        f"`streaming_utils.stop_all_streams()` before finishing."
    )
