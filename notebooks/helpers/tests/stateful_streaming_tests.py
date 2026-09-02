"""Checks for S03-stateful-streaming."""

from helpers import utils, event_stream

spark = utils.spark


def test_windowed_table_exists(**kwargs):
    cfg = utils.get_configs("events_per_minute")
    assert utils.table_exists(cfg["table_silver"]), (
        f"{cfg['table_silver']} does not exist. Step 1 builds a windowed aggregation."
    )


def test_windowed_table_has_window_bounds(**kwargs):
    cfg = utils.get_configs("events_per_minute")
    cols = set(utils.get_column_names(cfg["table_silver"]))
    assert {"window_start", "window_end"} <= cols, (
        f"Expected explicit `window_start` and `window_end` columns. Flattening the window "
        f"struct makes the table usable downstream. Columns: {sorted(cols)}"
    )


def test_windows_align_to_the_configured_size(**kwargs):
    cfg = utils.get_configs("events_per_minute")
    bad = spark.sql(f"""
        SELECT COUNT(*) AS c FROM {cfg['table_silver']}
        WHERE unix_timestamp(window_end) - unix_timestamp(window_start) <> 60
    """).collect()[0]["c"]
    assert bad == 0, (
        f"{bad} window(s) are not 60 seconds long. Step 1 specifies one-minute tumbling "
        f"windows — a sliding window would produce overlapping intervals."
    )


def test_windows_cover_the_replay_period(**kwargs):
    """The replay data spans a known, fixed interval, so the window count is predictable."""
    cfg = utils.get_configs("events_per_minute")
    manifest = event_stream.read_manifest()
    expected_minutes = manifest["files"] * manifest["minutes_per_file"]
    windows = spark.sql(
        f"SELECT COUNT(DISTINCT window_start) AS c FROM {cfg['table_silver']}"
    ).collect()[0]["c"]
    assert windows >= expected_minutes * 0.5, (
        f"Only {windows} distinct window(s) for roughly {expected_minutes} minutes of event "
        f"time. Check that you are windowing on `event_timestamp` and not on ingestion time."
    )


def test_watermark_dropped_late_events(**kwargs):
    """
    The generator stamps a slice of events well in the past on purpose. With a watermark
    in place, the windowed total must be smaller than the raw event count.
    """
    cfg_agg = utils.get_configs("events_per_minute")
    cfg_src = utils.get_configs("web_events")
    windowed = spark.sql(
        f"SELECT COALESCE(SUM(event_count), 0) AS c FROM {cfg_agg['table_silver']}"
    ).collect()[0]["c"]
    total = utils.get_table_row_count(cfg_src["table_silver"])
    assert windowed <= total, (
        f"The aggregate totals {windowed:,} events but the source only has {total:,}. "
        f"Something is double-counting."
    )
    assert windowed < total, (
        f"The aggregate totals exactly {windowed:,}, the same as the source. The seed data "
        f"contains events stamped ~{event_stream.LATE_MINUTES} minutes in the past, so a "
        f"working watermark should have dropped some. Is `withWatermark` applied before the "
        f"`groupBy`?"
    )


def test_enriched_table_exists(**kwargs):
    cfg = utils.get_configs("events_enriched")
    assert utils.table_exists(cfg["table_silver"]), (
        f"{cfg['table_silver']} does not exist. Step 2 joins the event stream to the static "
        f"product and user dimensions."
    )


def test_enriched_has_dimension_columns(**kwargs):
    cfg = utils.get_configs("events_enriched")
    cols = set(utils.get_column_names(cfg["table_silver"]))
    required = {"event_id", "event_timestamp", "event_type", "user_id",
                "product_id", "product_name", "product_category", "user_name"}
    missing = required - cols
    assert not missing, f"events_enriched is missing {sorted(missing)}."


def test_enriched_did_not_lose_events(**kwargs):
    """A stream-static join must be a left join — a missing dimension row is not a reason
    to throw away an event."""
    cfg_e = utils.get_configs("events_enriched")
    cfg_s = utils.get_configs("web_events")
    enriched = utils.get_table_row_count(cfg_e["table_silver"])
    source = utils.get_table_row_count(cfg_s["table_silver"])
    assert enriched == source, (
        f"The enriched table has {enriched:,} rows against {source:,} in silver. An inner join "
        f"drops events whose product_id is null — and `view` events have no product by design."
    )


def test_gold_funnel_exists(**kwargs):
    cfg = utils.get_configs("funnel_by_product")
    assert utils.table_exists(cfg["table_gold"]), (
        f"{cfg['table_gold']} does not exist. Step 3 builds the gold funnel table."
    )


def test_funnel_counts_are_monotonic(**kwargs):
    """Views >= clicks >= add_to_cart >= purchases, per product. A funnel that widens is a
    bug in the aggregation, not a marketing triumph."""
    cfg = utils.get_configs("funnel_by_product")
    bad = spark.sql(f"""
        SELECT COUNT(*) AS c FROM {cfg['table_gold']}
        WHERE clicks > views OR add_to_carts > clicks OR purchases > add_to_carts
    """).collect()[0]["c"]
    assert bad == 0, (
        f"{bad} product row(s) have a funnel stage larger than the stage above it. Check that "
        f"each stage counts distinct events and that the joins are not fanning out."
    )
