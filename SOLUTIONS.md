# Reference Solutions — Instructor Copy

Keep this off the student branch.

Reference answers, not the only correct ones. The checks are behavioural — a student who
writes `%sql` where the scaffold suggested Python still passes, and should.

---

# Batch track

## 01 — Batch Ingestion and Delta Engineering

### Step 0 — Initial Exploration

```python
# 1. List the sales files
sales_files = sorted(utils.list_files(raw_path))
print(f"{len(sales_files)} file(s):")
for f in sales_files:
    print(f"  {f}")

# 2. Compare the header of the first and last file
first_file, last_file = sales_files[0], sales_files[-1]

print(f"--- {first_file} ---")
print(dbutils.fs.head(f"{raw_path}/{first_file}", 500))

print(f"\n--- {last_file} ---")
print(dbutils.fs.head(f"{raw_path}/{last_file}", 500))

# 3. Read one delta file and inspect status
delta_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{raw_path}/{sales_files[1]}")   # a delta file, not day-0
)

delta_df.select("status").distinct().show()
```

### Step 1 — Idempotent ingestion

Auto Loader version (the one most students pick):

```python
def ingest_sales(full_table_name: str, data_path: str) -> None:
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .load(data_path)
        .withColumn("source_file", F.col("_metadata.file_name"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    (df.writeStream
       .format("delta")
       .option("checkpointLocation", checkpoint_path)
       .option("mergeSchema", "true")
       .outputMode("append")
       .trigger(availableNow=True)
       .toTable(full_table_name)
       .awaitTermination())
```

`COPY INTO` version:

```sql
CREATE TABLE IF NOT EXISTS {bronze} (
  sale_id INT, product_id INT, user_id INT, qty INT, price DOUBLE,
  status STRING, updated_at DATE, source_file STRING
) USING DELTA;

COPY INTO {bronze}
FROM (SELECT *, _metadata.file_name AS source_file FROM '{raw_path}')
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
```

**Expected answer:** with Auto Loader the memory is the checkpoint directory (RocksDB file
registry). With `COPY INTO` it is stored in the Delta table's own transaction log. Either
way it lives outside the SQL, which is why re-running the identical statement is safe.

### Step 1 — Idempotent ingestion

```python
# 1. Record the current row count
count_before = utils.get_table_row_count(bronze_table)
print(f"rows before second run: {count_before}")

# 2. Run the ingestion function again, unchanged
ingest_sales(bronze_table, raw_path)

# 3. Compare
count_after = utils.get_table_row_count(bronze_table)
print(f"rows after second run:  {count_after}")

assert count_before == count_after, (
    f"Row count changed from {count_before} to {count_after} — "
    f"the second run reprocessed files it should have skipped."
)
print("Idempotent: row count unchanged.")

# 4. Look at DESCRIBE HISTORY
display(spark.sql(f"DESCRIBE HISTORY {bronze_table}"))


```

### Step 3 — Schema evolution

Both sides matter, and this is the most common partial failure:

- Read side tells the reader to expect new columns (`schemaEvolutionMode`, or
  `FORMAT_OPTIONS mergeSchema`).
- Write side tells Delta to widen the table (`mergeSchema` on write, or `COPY_OPTIONS`).

Get the write side only and the column is added but always null — which is exactly what
`test_schema_evolution_absorbed_region` catches.

```python
cols = utils.get_column_names(bronze_table)
print("region" in cols)  # should be True

spark.sql(f"""
    SELECT
        SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END) AS null_region,
        SUM(CASE WHEN region IS NOT NULL THEN 1 ELSE 0 END) AS has_region
    FROM {bronze_table}
""").show()
```

**Expected answer on type widening:** `addNewColumns` adds columns, it does not change the
type of an existing one. A `price` of `"19.99 USD"` against an inferred `DOUBLE` goes to
`_rescued_data` as JSON. Students should know to look there.

### Step 4

```python
def create_silver_sales(full_table_name: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            sale_id INT, product_id INT, user_id INT,
            quantity INT, price DOUBLE, region STRING,
            _is_active BOOLEAN, _created_at DATE, _updated_at DATE, _source_file STRING
        ) USING DELTA
    """)
```
### Step 5 — CDC merge

```python
def cdc_merge(bronze: str, silver: str, filter_date: str) -> None:
    spark.sql(f"""
        MERGE INTO {silver} AS t
        USING (
            SELECT sale_id, product_id, user_id, qty AS quantity, price, region,
                   status, updated_at, source_file
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY sale_id ORDER BY updated_at DESC, source_file DESC
                ) AS rn
                FROM {bronze}
                WHERE updated_at = DATE('{filter_date}')
            ) WHERE rn = 1
        ) AS s
        ON t.sale_id = s.sale_id

        WHEN MATCHED AND lower(s.status) = 'delete' THEN UPDATE SET
            t._is_active = false, t._updated_at = s.updated_at, t._source_file = s.source_file

        WHEN MATCHED THEN UPDATE SET
            t.product_id = s.product_id, t.user_id = s.user_id, t.quantity = s.quantity,
            t.price = s.price, t.region = s.region, t._is_active = true,
            t._updated_at = s.updated_at, t._source_file = s.source_file

        WHEN NOT MATCHED AND lower(s.status) <> 'delete' THEN INSERT (
            sale_id, product_id, user_id, quantity, price, region,
            _is_active, _created_at, _updated_at, _source_file
        ) VALUES (
            s.sale_id, s.product_id, s.user_id, s.quantity, s.price, s.region,
            true, s.updated_at, s.updated_at, s.source_file
        )
    """)


def all_change_dates(bronze: str) -> list:
    return [r[0].isoformat() for r in spark.sql(
        f"SELECT DISTINCT updated_at FROM {bronze} ORDER BY updated_at"
    ).collect()]

for filter_date in all_change_dates(bronze_table):
    cdc_merge(bronze_table, silver_table, filter_date)
    print(f"merged {filter_date}")

print(f"\n{utils.get_table_row_count(silver_table)} rows in silver")
```

**Common failures**

- Omitting the `ROW_NUMBER` dedup → `Cannot perform Merge as multiple source rows matched`.
  Worth letting them hit.
- Putting the delete clause *after* the general `WHEN MATCHED` — clause order matters, first
  match wins, so the general clause swallows the deletes. `_is_active` then never goes false
  and `test_deletes_became_soft_deletes` fails.
- Overwriting `_created_at` on update. Silent, and the check does not catch it — look for it
  in review.

**Expected answers:** `_created_at` unchanged is what lets gold answer "cohort by first
purchase date". Re-running a day is a no-op in effect because the source rows for that day
are identical, so every update writes the same values.

### Step 6 — Validate

```python
# 1. Active vs soft-deleted
display(spark.sql(f"""
    SELECT _is_active, COUNT(*) AS n
    FROM {silver_table}
    GROUP BY _is_active
"""))

# 2. No sale_id duplicated
dupes = spark.sql(f"""
    SELECT sale_id, COUNT(*) AS c
    FROM {silver_table}
    GROUP BY sale_id
    HAVING COUNT(*) > 1
""")
assert dupes.count() == 0, "Found duplicated sale_id in silver"

# 3. One MERGE per day
display(spark.sql(f"DESCRIBE HISTORY {silver_table}"))
merge_days = spark.sql(f"""
    SELECT COUNT(*) AS merges
    FROM (DESCRIBE HISTORY {silver_table})
    WHERE operation = 'MERGE'
""")
display(merge_days)
```

---



