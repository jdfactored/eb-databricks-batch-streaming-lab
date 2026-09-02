"""Checks for B01-batch-ingestion-and-delta."""

from helpers import utils

spark = utils.spark


def test_bronze_sales_exists(**kwargs):
    cfg = utils.get_configs("sales")
    assert utils.table_exists(cfg["table_bronze"]), (
        f"{cfg['table_bronze']} does not exist. See Step 1."
    )


def test_bronze_carries_file_metadata(**kwargs):
    """`_metadata.file_name` is what makes a load traceable and re-runnable."""
    cfg = utils.get_configs("sales")
    cols = utils.get_column_names(cfg["table_bronze"])
    assert "source_file" in cols, (
        f"Bronze is missing `source_file`. Step 1 asks you to capture "
        f"`_metadata.file_name` at ingestion. Columns found: {cols}"
    )


def test_bronze_loaded_all_files(**kwargs):
    cfg = utils.get_configs("sales")
    on_disk = {f for f in utils.list_files(cfg["raw_path"]) if f.endswith(".csv")}
    loaded = {
        row["source_file"]
        for row in spark.sql(f"SELECT DISTINCT source_file FROM {cfg['table_bronze']}").collect()
    }
    # source_file may be a full path; compare on the trailing name.
    loaded_names = {p.split("/")[-1] for p in loaded if p}
    missing = on_disk - loaded_names
    assert not missing, (
        f"{len(missing)} sales file(s) never reached bronze: {sorted(missing)}."
    )


def test_ingestion_is_idempotent(**kwargs):
    """
    Step 2 asks you to run the load a second time. COPY INTO and Auto Loader both track
    which files they have already consumed, so the row count must not change.
    """
    cfg = utils.get_configs("sales")
    rows = utils.get_table_row_count(cfg["table_bronze"])
    files = len([f for f in utils.list_files(cfg["raw_path"]) if f.endswith(".csv")])
    per_file = spark.sql(f"""
        SELECT source_file, COUNT(*) AS c
        FROM {cfg['table_bronze']}
        GROUP BY source_file
        HAVING COUNT(*) = 0
    """).count()
    assert per_file == 0, "Unexpected empty file group."
    duplicated = spark.sql(f"""
        SELECT sale_id, source_file, COUNT(*) AS c
        FROM {cfg['table_bronze']}
        GROUP BY sale_id, source_file
        HAVING COUNT(*) > 1
    """).count()
    assert duplicated == 0, (
        f"{duplicated} (sale_id, source_file) pair(s) appear more than once — the same file "
        f"was ingested twice. Idempotent ingestion should have skipped it. "
        f"({rows} rows from {files} files)"
    )


def test_schema_evolution_absorbed_region(**kwargs):
    """`region` appears partway through the sales deliveries."""
    cfg = utils.get_configs("sales")
    cols = utils.get_column_names(cfg["table_bronze"])
    assert "region" in cols, (
        f"Bronze has no `region` column. The later sales files add it — Step 3 asks you to "
        f"handle that without dropping the rows. Columns found: {cols}"
    )
    populated = spark.sql(
        f"SELECT COUNT(*) AS c FROM {cfg['table_bronze']} WHERE region IS NOT NULL"
    ).collect()[0]["c"]
    assert populated > 0, (
        "`region` exists but is null everywhere — the column was added to the table but the "
        "values were not read. Check your schema-evolution option on the *read* side."
    )


def test_silver_sales_exists(**kwargs):
    cfg = utils.get_configs("sales")
    assert utils.table_exists(cfg["table_silver"]), (
        f"{cfg['table_silver']} does not exist. See Step 4."
    )


def test_silver_has_cdc_audit_columns(**kwargs):
    cfg = utils.get_configs("sales")
    cols = set(utils.get_column_names(cfg["table_silver"]))
    required = {"sale_id", "product_id", "user_id", "quantity", "price",
                "_is_active", "_created_at", "_updated_at"}
    missing = required - cols
    assert not missing, (
        f"Silver is missing {sorted(missing)}. Step 4 specifies the target schema — note "
        f"`qty` is renamed to `quantity`, and `status`/`updated_at` do not survive the merge."
    )
    assert "status" not in cols, (
        "`status` is a CDC operation marker, not data. It should drive the MERGE and then "
        "be dropped, not stored in silver."
    )


def test_silver_one_row_per_sale(**kwargs):
    cfg = utils.get_configs("sales")
    dupes = spark.sql(f"""
        SELECT sale_id FROM {cfg['table_silver']}
        GROUP BY sale_id HAVING COUNT(*) > 1
    """).count()
    assert dupes == 0, (
        f"{dupes} sale_id value(s) appear more than once in silver. The MERGE key should be "
        f"sale_id alone."
    )


def test_deletes_became_soft_deletes(**kwargs):
    """The scenario keeps deleted sales for auditing rather than removing the row."""
    cfg = utils.get_configs("sales")
    deleted_in_source = spark.sql(f"""
        SELECT COUNT(DISTINCT sale_id) AS c FROM {cfg['table_bronze']}
        WHERE lower(status) = 'delete'
    """).collect()[0]["c"]
    assert deleted_in_source > 0, (
        "No delete records found in bronze — the seed data should contain them. "
        "Check that every sales file was loaded."
    )
    inactive = spark.sql(
        f"SELECT COUNT(*) AS c FROM {cfg['table_silver']} WHERE _is_active = false"
    ).collect()[0]["c"]
    assert inactive == deleted_in_source, (
        f"Bronze marks {deleted_in_source} sale(s) as deleted but silver has {inactive} rows "
        f"with _is_active = false. Deletes should flip the flag, not remove the row."
    )


def test_merge_was_used(**kwargs):
    cfg = utils.get_configs("sales")
    ops = utils.get_history_operations(cfg["table_silver"])
    assert "MERGE" in ops, (
        f"No MERGE in the history of {cfg['table_silver']} (found {sorted(set(ops))}). "
        f"Step 4 asks for MERGE INTO applied per day, not a single CREATE OR REPLACE."
    )


def test_merge_ran_per_day(**kwargs):
    """One merge per daily delta — that is what makes a replay of one day cheap."""
    cfg = utils.get_configs("sales")
    merges = [op for op in utils.get_history_operations(cfg["table_silver"]) if op == "MERGE"]
    days = spark.sql(
        f"SELECT COUNT(DISTINCT updated_at) AS c FROM {cfg['table_bronze']}"
    ).collect()[0]["c"]
    assert len(merges) >= days, (
        f"Found {len(merges)} MERGE operation(s) for {days} distinct source day(s). "
        f"Step 5 asks you to apply the CDC feed one day at a time."
    )
