# Reference Solutions — Instructor Copy

Keep this off the student branch.

Reference answers, not the only correct ones. The checks are behavioural — a student who
writes `%sql` where the scaffold suggested Python still passes, and should.

---

# Batch track

## B01 — Batch Ingestion and Delta Engineering

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

# Streaming track

## S01 — Streaming Fundamentals

### Step 1

```python
df = event_stream.rate_events(rows_per_second=20)
df.printSchema()
print(df.isStreaming)
df.count()   # AnalysisException: Queries with streaming sources must be executed with writeStream.start()
```

**Expected answer:** a stream has no end, so "how many rows" has no answer at any instant.
Aggregations over a stream must be incremental and must emit under an output mode.

### Step 2

```python
query = (df.writeStream.format("delta")
         .option("checkpointLocation", rate_checkpoint)
         .outputMode("append")
         .trigger(availableNow=True)
         .toTable(rate_table))
query.awaitTermination()

print(spark.table(rate_table).count())

display(spark.sql(f"DESCRIBE HISTORY {rate_table}"))
```

**Expected answer on `DESCRIBE HISTORY`:** the `operation` column reads
`STREAMING UPDATE`, not `WRITE`. A batch `saveAsTable`/`insertInto` commits as a single
`WRITE`; a streaming sink commits once per micro-batch as `STREAMING UPDATE` and the
`operationMetrics` for that row includes `numOutputRows` for just that batch, not the
whole table. Even though `availableNow` looks like "run once", it can still span several
micro-batches internally — check `len(query.recentProgress)` if the history shows more
than one `STREAMING UPDATE` row and the count should still explain itself.

### Step 3

```python
query = (df.writeStream.format("delta")
         .option("checkpointLocation", rate_checkpoint)
         .outputMode("append")
         .trigger(processingTime="5 seconds")
         .toTable(rate_table))

streaming_utils.await_batches(query, batches=4)

streaming_utils.show_progress(query)

query.stop()
```

**Expected answers:** the row count only grows, it does not restart, because the checkpoint
recorded the offset. The rate source is virtual — while stopped, no rows were generated at
all; the source resumes from the offset. This differs from a file source, where files
genuinely accumulate on disk. Worth drawing out, students conflate the two.

### Step 4

Append on an unwatermarked aggregation raises
`Append output mode not supported when there are streaming aggregations`. Complete mode works.

**Expected answers:** complete mode stops being viable when the result table no longer fits
comfortably in memory and cannot be rewritten each batch — thousands of groups is fine,
millions is not; switch to `update` with a merge sink. Raw events in append mode are fine
because each row is final on arrival; an aggregate is never final without a watermark.

### Step 5

**Expected answers:** the `offsets` directory holds one file per batch recording the source
position, and `commits` records which batches finished. On restart Spark reads the last
commit and resumes. Deleting the checkpoint and restarting against the same table gives you
duplicates for a file source (everything reprocessed and appended) — for the rate source it
restarts from zero. Exactly-once comes from the *sink*: the Delta commit and the offset
advance are made atomic, so a replayed batch does not double-write.

---

## S02 — Auto Loader

### Step 2

```python
def read_events_stream(source_path, schema_location):
    return (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.maxFilesPerTrigger", 1)
        .load(source_path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_name")))


def write_events_stream(df, checkpoint, table):
    return (df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(table))
```

Students run it, it stops at file 009 with `UnknownFieldException`, they restart the same
cell, it completes. Expect one round of confusion — the notebook warns them but they read the
warning after the failure.

**Expected answers:** the restart reads the schema location, which now records the new
column. For unattended 3am pipelines, `addNewColumns` is still usually right, but only if the
job has automatic retry configured and an alert fires — otherwise use `rescue`, which never
stops, and monitor the rescued column.

### Step 5

```python
w = Window.partitionBy("event_id").orderBy(F.col("_ingested_at").asc())
(spark.table(bronze_table)
 .withColumn("rn", F.row_number().over(w))
 .filter("rn = 1").drop("rn", "_rescued_data")
 .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
 .write.mode("overwrite").saveAsTable(silver_table))
```

Expected count: 5835 (6000 total minus 165 replayed duplicates). Keeping first or last does
not change the count here because the duplicates are byte-identical — but the student should
say which they chose and why. If payloads differed, last-write-wins is the usual choice, and
the honest answer is that you need a source-side sequence number rather than arrival order.

---

## S03 — Stateful Streaming

### Step 1

```python
def events_per_minute(source_table, watermark="5 minutes"):
    return (spark.readStream.table(source_table)
        .withWatermark("event_timestamp", watermark)
        .groupBy(F.window("event_timestamp", "1 minute"), "event_type")
        .agg(F.count("*").alias("event_count"))
        .select(F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "event_type", "event_count"))

(events_per_minute(events_silver).writeStream
 .format("delta").option("checkpointLocation", window_checkpoint)
 .outputMode("append").trigger(availableNow=True)
 .toTable(window_table).awaitTermination())
```

Windowed total lands around 5,700 against 5,835 in silver — the gap is the late events. If a
student's totals match exactly, the watermark is after the `groupBy` or missing.

**Expected answers:** append became legal because the watermark tells Spark when a window can
never change again. A 60-minute watermark recovers nearly all the late events at the cost of
holding an hour of window state and delaying every window's emission by an hour.

### Step 2

```python
def enrich_events(source_table, products, users):
    p = spark.table(products).select(
        F.col("product_id"), F.col("name").alias("product_name"),
        F.col("category").alias("product_category"), F.col("price").alias("product_price"))
    u = spark.table(users).select(
        F.col("user_id"), F.col("name").alias("user_name"))
    return (spark.readStream.table(source_table)
            .join(p, "product_id", "left")
            .join(u, "user_id", "left"))
```

The inner-join mistake is common and the check catches it — over half the events are `view`
with a null `product_id`.

**Expected answers:** re-reading the static side each batch costs a scan per batch, visible
as batch duration that does not fall when input rows do. An event processed before its
product existed never gets enriched — you would need to reprocess, or do the lookup at query
time in gold instead. For point-in-time price, join to the SCD2 dimension from B04 on
`event_date BETWEEN valid_from AND COALESCE(valid_to, '9999-12-31')`.

### Step 3

```python
def upsert_funnel(batch_df, batch_id):
    agg = (batch_df.groupBy("product_id", "product_name").agg(
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
        F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("clicks"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_carts"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases")))
    agg.createOrReplaceTempView("batch_agg")
    batch_df.sparkSession.sql(f"""
        MERGE INTO {funnel_table} AS t USING batch_agg AS s
        ON t.product_id <=> s.product_id
        WHEN MATCHED THEN UPDATE SET
            t.views = t.views + s.views, t.clicks = t.clicks + s.clicks,
            t.add_to_carts = t.add_to_carts + s.add_to_carts,
            t.purchases = t.purchases + s.purchases,
            t.updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT *
    """)
```

Note `<=>` rather than `=` for the null-safe product_id comparison — `view` events have a
null product and a plain `=` never matches them, so they accumulate as new rows every batch.

**Expected answer on the guard:** keep an `applied_batches` table, and inside `foreachBatch`
check whether `batch_id` is already present before merging, inserting the id in the same
transaction. Accumulating with `+` is correct here because each batch carries only new events;
replacing would discard everything but the last batch.

---

## S04 — Monitoring and Governance

### Step 1

```python
rows = streaming_utils.progress_summary(query)
(spark.createDataFrame(rows)
 .withColumn("query_name", F.lit(query.name or "replay"))
 .withColumn("run_timestamp", F.current_timestamp())
 .write.mode("append").saveAsTable(metrics_table))
```

**Expected answer:** alert on `batchDuration > trigger interval` sustained over several
batches — it is the earliest signal and it is self-calibrating, unlike an absolute row-rate
threshold that goes stale the moment traffic grows.

### Step 2

Expect roughly: 12 batches for `maxFilesPerTrigger = 1`, 2 for `= 6`; total duration lower
for B, per-batch duration higher. Total rows must be 6000 for both — a mismatch means a
shared checkpoint, which is the bug the notebook warns about.

**Expected answers:** hourly dashboard takes run B, ten-second fraud check takes run A.
`maxBytesPerTrigger` matters when file sizes vary wildly — twelve files could be 12 MB or 12
GB, and only the byte bound protects you from an oversized batch.

### Steps 3–4

```sql
COMMENT ON TABLE {enriched} IS
  'Clickstream events joined to product and user dimensions. Grain: one row per event_id. Owner: data-eng.';

ALTER TABLE {enriched} ALTER COLUMN user_name SET TAGS ('pii' = 'true');

CREATE OR REPLACE VIEW {gold}.v_events_masked AS
SELECT event_id, event_timestamp, event_type, device_type,
       left(session_id, 8) AS session_prefix,
       user_id, product_id, product_name, product_category, product_price,
       CASE WHEN is_account_group_member('pii_readers') THEN user_name
            ELSE '***' END AS user_name
FROM {enriched};
```

**Expected answers:** a tag is useful when something consumes it — a scheduled job that
applies masks to every tagged column, or an access review that reports on them. A plain view
reflects new rows immediately since it is just stored SQL; a materialized view would serve
stale data until refreshed, which is a real trade for a continuously appended source.
Truncating `session_id` keeps sessionisation working (the prefix is still a stable grouping
key) while making cross-dataset re-identification harder; hashing keeps uniqueness but breaks
prefix grouping; dropping breaks funnel analysis entirely.

### Wrap-up questions — what a good answer contains

1. **Exactly-once.** The source provides replayable offsets; the sink provides atomic
   commit-plus-offset-advance. `foreachBatch` breaks the chain because your arbitrary code
   commits separately from the offset, so a crash between the two replays your side effect.
   The fix is idempotency keyed on `batch_id`.
2. **Falling behind.** `batchDuration` vs trigger interval first, then `numInputRows` (is
   input genuinely up?), then `stateOperators.numRowsTotal` (unbounded state), then the source
   backlog, then file sizes at the source (a flood of tiny files is a classic cause). Look for
   an ordering that starts with symptoms and narrows.
3. **30-day reprocess.** Write to a *new* target table with a *new* checkpoint, verify it,
   then swap the consumers. Do not delete the checkpoint of the running pipeline and hope. If
   you must reuse the target, `replaceWhere` the affected range. Mention downstream: anything
   already computed off the bad data needs the same treatment.
4. **Broker vs Auto Loader.** For the broker: sub-second latency, many independent consumers
   at different offsets, backpressure, and producers that cannot write files. Against: another
   system to run, secure and pay for, plus retention limits that Delta does not have. Look for
   an answer that names a latency figure and a consumer count rather than gesturing at
   "scale".
5. **45-minute-late event, 10-minute watermark.** It is dropped — its window state was already
   evicted, and it is not counted anywhere. If it mattered: widen the watermark (costs memory
   and delays emission), or route late records to a side table and reconcile in a nightly
   batch, which is what most production systems actually do.
