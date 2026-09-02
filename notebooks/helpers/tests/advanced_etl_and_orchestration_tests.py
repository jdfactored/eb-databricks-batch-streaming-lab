"""Checks for B04-advanced-etl-and-orchestration."""

from helpers import utils

spark = utils.spark


def test_scd2_table_exists(**kwargs):
    cfg = utils.get_configs("dim_product")
    assert utils.table_exists(cfg["table_gold"]), (
        f"{cfg['table_gold']} does not exist. Step 1 builds a Type 2 product dimension."
    )


def test_scd2_has_required_columns(**kwargs):
    cfg = utils.get_configs("dim_product")
    cols = set(utils.get_column_names(cfg["table_gold"]))
    required = {"product_id", "name", "category", "price",
                "valid_from", "valid_to", "is_current", "surrogate_key"}
    missing = required - cols
    assert not missing, f"dim_product is missing {sorted(missing)}."


def test_scd2_exactly_one_current_row_per_key(**kwargs):
    cfg = utils.get_configs("dim_product")
    bad = spark.sql(f"""
        SELECT product_id, COUNT(*) AS c
        FROM {cfg['table_gold']}
        WHERE is_current = true
        GROUP BY product_id
        HAVING COUNT(*) <> 1
    """).count()
    assert bad == 0, (
        f"{bad} product(s) have zero or more than one current row. Closing the old version and "
        f"opening the new one must happen together."
    )


def test_scd2_tracked_at_least_one_change(**kwargs):
    cfg = utils.get_configs("dim_product")
    versioned = spark.sql(f"""
        SELECT product_id FROM {cfg['table_gold']}
        GROUP BY product_id HAVING COUNT(*) > 1
    """).count()
    assert versioned > 0, (
        "No product has more than one row, so no history was captured. The seed snapshots do "
        "contain price changes — check that your comparison detects them."
    )


def test_scd2_intervals_do_not_overlap(**kwargs):
    cfg = utils.get_configs("dim_product")
    overlaps = spark.sql(f"""
        SELECT a.product_id
        FROM {cfg['table_gold']} a
        JOIN {cfg['table_gold']} b
          ON a.product_id = b.product_id
         AND a.surrogate_key <> b.surrogate_key
        WHERE a.valid_from < COALESCE(b.valid_to, DATE'9999-12-31')
          AND COALESCE(a.valid_to, DATE'9999-12-31') > b.valid_from
    """).count()
    assert overlaps == 0, (
        f"{overlaps} overlapping validity interval(s). For any product and any date there must "
        f"be exactly one row whose interval covers that date."
    )


def test_backfill_is_idempotent(**kwargs):
    """Step 2 asks you to re-run one day's load. Row counts must not move."""
    cfg = utils.get_configs("fact_sales_daily")
    assert utils.table_exists(cfg["table_gold"]), (
        f"{cfg['table_gold']} does not exist. See Step 2."
    )
    dupes = spark.sql(f"""
        SELECT sale_date, product_id, COUNT(*) AS c
        FROM {cfg['table_gold']}
        GROUP BY sale_date, product_id
        HAVING COUNT(*) > 1
    """).count()
    assert dupes == 0, (
        f"{dupes} (sale_date, product_id) pair(s) are duplicated. A backfill that appends "
        f"instead of replacing its own partition doubles data every time it is re-run."
    )


def test_backfill_log_exists(**kwargs):
    cfg = utils.get_configs("pipeline_runs")
    assert utils.table_exists(cfg["table_gold"]), (
        f"{cfg['table_gold']} does not exist. Step 3 asks for a run log so the job knows what "
        f"it has already processed."
    )
    cols = set(utils.get_column_names(cfg["table_gold"]))
    required = {"run_id", "processed_date", "rows_written", "status", "run_timestamp"}
    missing = required - cols
    assert not missing, f"pipeline_runs is missing {sorted(missing)}."


def test_run_log_records_a_rerun(**kwargs):
    cfg = utils.get_configs("pipeline_runs")
    reruns = spark.sql(f"""
        SELECT processed_date FROM {cfg['table_gold']}
        GROUP BY processed_date HAVING COUNT(*) > 1
    """).count()
    assert reruns > 0, (
        "No date appears twice in the run log, so nothing was ever re-run. Step 2 asks you to "
        "reprocess a day and confirm the fact table is unchanged."
    )
