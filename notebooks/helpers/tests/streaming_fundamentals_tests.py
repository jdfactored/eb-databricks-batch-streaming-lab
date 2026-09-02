"""Checks for S01-streaming-fundamentals (rate source — no files involved)."""

from helpers import utils

spark = utils.spark


def test_rate_sink_exists(**kwargs):
    cfg = utils.get_configs("rate_events")
    assert utils.table_exists(cfg["table_bronze"]), (
        f"{cfg['table_bronze']} does not exist. Step 2 writes the rate stream to Delta."
    )


def test_rate_sink_has_rows(**kwargs):
    cfg = utils.get_configs("rate_events")
    rows = utils.get_table_row_count(cfg["table_bronze"])
    assert rows > 0, (
        f"{cfg['table_bronze']} is empty. Did the query run long enough before you stopped it?"
    )


def test_rate_sink_written_by_streaming(**kwargs):
    """A streaming write shows up as STREAMING UPDATE, not WRITE."""
    cfg = utils.get_configs("rate_events")
    ops = utils.get_history_operations(cfg["table_bronze"])
    assert any("STREAMING" in op.upper() for op in ops), (
        f"History for {cfg['table_bronze']} shows {sorted(set(ops))} — no streaming write. "
        f"The table was populated with a batch write, not `writeStream`."
    )


def test_multiple_micro_batches(**kwargs):
    """More than one commit means the student saw the stream actually iterate."""
    cfg = utils.get_configs("rate_events")
    streaming_commits = [
        op for op in utils.get_history_operations(cfg["table_bronze"])
        if "STREAMING" in op.upper()
    ]
    assert len(streaming_commits) >= 2, (
        f"Only {len(streaming_commits)} streaming commit(s). Step 3 asks you to run with a "
        f"`processingTime` trigger for long enough to see several micro-batches."
    )


def test_checkpoint_directory_created(**kwargs):
    cfg = utils.get_configs("rate_events")
    assert utils.path_exists(cfg["checkpoint_path"]), (
        f"No checkpoint at {cfg['checkpoint_path']}. Every streaming write needs one — it is "
        f"where the offsets that make restart safe are stored."
    )
    contents = [f.name for f in utils.dbutils.fs.ls(cfg["checkpoint_path"])]
    assert any("offset" in c for c in contents), (
        f"The checkpoint directory has no `offsets` folder. Contents: {contents}"
    )


def test_aggregate_table_exists(**kwargs):
    cfg = utils.get_configs("rate_events_by_type")
    assert utils.table_exists(cfg["table_silver"]), (
        f"{cfg['table_silver']} does not exist. Step 4 asks for a complete-mode aggregation."
    )


def test_aggregate_covers_all_event_types(**kwargs):
    cfg = utils.get_configs("rate_events_by_type")
    types = {
        r["event_type"]
        for r in spark.sql(f"SELECT DISTINCT event_type FROM {cfg['table_silver']}").collect()
    }
    assert len(types) >= 3, (
        f"The aggregate only contains {sorted(types)}. A complete-mode aggregation restates "
        f"every group each batch, so all event types should be present."
    )
