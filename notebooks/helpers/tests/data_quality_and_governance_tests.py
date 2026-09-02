"""Checks for B02-data-quality-and-governance."""

from helpers import utils

spark = utils.spark


def test_quarantine_table_exists(**kwargs):
    cfg = utils.get_configs("sales_quarantine")
    assert utils.table_exists(cfg["table_silver"]), (
        f"{cfg['table_silver']} does not exist. Step 2 asks you to route failing rows "
        f"to a quarantine table instead of dropping them."
    )


def test_quarantine_records_a_reason(**kwargs):
    cfg = utils.get_configs("sales_quarantine")
    cols = set(utils.get_column_names(cfg["table_silver"]))
    assert "dq_failed_rules" in cols, (
        f"Quarantine is missing `dq_failed_rules`. A quarantined row nobody can diagnose is "
        f"just a deleted row with extra steps. Columns found: {sorted(cols)}"
    )
    assert "dq_quarantined_at" in cols, (
        "Quarantine is missing `dq_quarantined_at` — you need to know when a row was rejected."
    )


def test_quarantine_captured_rows(**kwargs):
    cfg = utils.get_configs("sales_quarantine")
    count = utils.get_table_row_count(cfg["table_silver"])
    assert count > 0, (
        "The quarantine table is empty. The seed data contains rows that violate at least one "
        "of the rules in Step 1 — if nothing was caught, your rules are not being applied."
    )


def test_no_rows_were_silently_lost(**kwargs):
    """clean + quarantined must equal what came in. Dropping rows is never acceptable."""
    clean = utils.get_configs("sales_validated")
    quarantine = utils.get_configs("sales_quarantine")
    source = utils.get_configs("sales")

    total_in = spark.sql(
        f"SELECT COUNT(*) AS c FROM {source['table_silver']}"
    ).collect()[0]["c"]
    total_out = (
        utils.get_table_row_count(clean["table_silver"])
        + utils.get_table_row_count(quarantine["table_silver"])
    )
    assert total_in == total_out, (
        f"{total_in} rows went in, {total_out} came out (clean + quarantined). "
        f"{abs(total_in - total_out)} row(s) vanished. Every input row must land somewhere."
    )


def test_validated_table_passes_its_own_rules(**kwargs):
    cfg = utils.get_configs("sales_validated")
    bad = spark.sql(f"""
        SELECT COUNT(*) AS c FROM {cfg['table_silver']}
        WHERE price <= 0 OR quantity <= 0 OR product_id IS NULL OR user_id IS NULL
    """).collect()[0]["c"]
    assert bad == 0, (
        f"{bad} row(s) in the validated table still violate the rules they were checked "
        f"against. The filter and the quarantine predicate have drifted apart — derive both "
        f"from one rule definition."
    )


def test_dq_metrics_table_exists(**kwargs):
    cfg = utils.get_configs("dq_metrics")
    assert utils.table_exists(cfg["table_gold"]), (
        f"{cfg['table_gold']} does not exist. Step 3 asks for a metrics table so quality is "
        f"trended over time rather than checked once."
    )


def test_dq_metrics_are_populated(**kwargs):
    cfg = utils.get_configs("dq_metrics")
    cols = set(utils.get_column_names(cfg["table_gold"]))
    required = {"run_timestamp", "rule_name", "rows_checked", "rows_failed"}
    missing = required - cols
    assert not missing, f"dq_metrics is missing {sorted(missing)}."
    assert utils.get_table_row_count(cfg["table_gold"]) > 0, "dq_metrics is empty."


def test_constraints_on_validated_table(**kwargs):
    cfg = utils.get_configs("sales_validated")
    constraints = [
        k for k in utils.get_table_properties(cfg["table_silver"])
        if k.startswith("delta.constraints.")
    ]
    assert len(constraints) >= 2, (
        f"Found {len(constraints)} CHECK constraint(s) on {cfg['table_silver']}, expected at "
        f"least 2. Step 4 asks you to enforce at the table level what Step 1 checked in code."
    )


def test_pii_view_exists(**kwargs):
    cfg = utils.get_configs()
    view = f"{cfg['catalog']}.{cfg['schema_gold']}.v_sales_masked"
    assert utils.table_exists(view), (
        f"{view} does not exist. Step 5 asks for a governed view that masks PII for readers "
        f"who are not in the privileged group."
    )


def test_pii_view_actually_masks(**kwargs):
    """The view must not expose raw email to an unprivileged caller."""
    cfg = utils.get_configs()
    view = f"{cfg['catalog']}.{cfg['schema_gold']}.v_sales_masked"
    sql = spark.sql(f"SHOW CREATE TABLE {view}").collect()[0][0].lower()
    assert "is_account_group_member" in sql or "is_member" in sql, (
        "The view definition contains no group check. Masking that everyone can see through "
        "is decoration — use is_account_group_member() to decide what each caller gets."
    )
