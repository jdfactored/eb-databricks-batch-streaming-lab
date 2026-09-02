[template-version]: # (0.0.2)
# Databricks for Data Engineering — Batch & Streaming Lab

**Tools**: Databricks, Unity Catalog, Delta Lake, PySpark, Structured Streaming, Auto Loader, Asset Bundles
**Course ID**: `course-v1:Factored+DE013+2026T1`
**Time**: about 9 hours of hands-on work, designed for a two-week window
**Compute**: Serverless throughout
**Prerequisite**: the Fundamentals lab (`DE002+DBX001`), or equivalent comfort with Unity Catalog, Delta history and `MERGE`

---

## What this lab is

Two independent tracks over the same e-commerce dataset.

**Batch** takes a daily sales CDC feed from raw files to a governed gold layer, then
schedules it. **Streaming** takes a clickstream from a synthetic source to a windowed,
enriched, governed set of tables.

You can do them in either order. They share the setup notebook and the same catalog, and
S03 joins the streaming events to dimensions the batch track builds — but that notebook
gives you a fallback if you have not done B04 yet.

## Notebooks

```
notebooks/
  00-setup.ipynb                            run once, no code required        5 min

  B01-batch-ingestion-and-delta.ipynb       idempotent ingestion, CDC MERGE   75 min
  B02-data-quality-and-governance.ipynb     rules, quarantine, masking        60 min
  B03-performance-tuning.ipynb              measure, cluster, read plans      60 min
  B04-advanced-etl-and-orchestration.ipynb  SCD2, backfills, Jobs + bundle    75 min

  S01-streaming-fundamentals.ipynb          triggers, modes, checkpoints      50 min
  S02-ingesting-streams-autoloader.ipynb    Auto Loader, schema drift, dedup  70 min
  S03-stateful-streaming.ipynb              watermarks, joins, foreachBatch   75 min
  S04-monitoring-and-governance.ipynb       metrics, tuning, UC governance    65 min

  99-cleanup.ipynb                          run only after grading             2 min
```

Lesson mapping:

| Course lesson | Notebook |
| :-- | :-- |
| Batch Processing Fundamentals | B01 |
| Ingesting Batch Data | B01 |
| Data Engineering with Delta Lake | B01 |
| Managing Data Quality Governance | B02 |
| Performance Tuning Optimization | B03 |
| Advanced ETL Patterns | B04 |
| Orchestrating Batch Pipelines | B04 |
| Streaming Fundamentals | S01 |
| Ingesting Data Streams | S02 |
| Data Engineering with Structured Streaming | S03 |
| Managing Streaming Tables with Delta Lake | S03 |
| Monitoring and Optimizing Streams | S04 |
| Governance and Security for Streaming Data | S04 |

## Getting started

1. Import this folder into your Databricks workspace.
2. Attach `00-setup.ipynb` to **Serverless** and run it top to bottom.
3. Start with `B01` or `S01`.

Each step gives you a short explanation and a **TO DO** cell. Function signatures are
suggestions — `%sql` cells are fine where you prefer them. The checks look at the objects
you produced, not at how you produced them.

The last cell of each notebook runs its checks:

```python
from helpers import test_runner
test_runner.run("S02-ingesting-streams-autoloader")
```

## There is no Kafka in this lab

Streaming here uses four sources, each chosen for a different reason.

| Source | Where | Why it is the right one there |
| :-- | :-- | :-- |
| Spark `rate` source | S01 | No files, no volume, nothing to misconfigure. When something breaks, it is your streaming code. |
| Deterministic file replay | S02, S03, S04 | Twelve pre-generated JSON files read one per micro-batch. Identical every run, so the checks can assert exact counts. |
| Live drip generator | optional, any notebook | `event_stream.start_drip()` writes files on a background thread while your stream runs. The only source that feels genuinely live, and the only non-deterministic one. |
| Delta table as a stream | S03 | Versions act as offsets, so bronze behaves like a log. This is how most Lakehouse pipelines work without a broker at all. |

The replay data carries three deliberate defects: duplicate event ids across files, a
`referrer_url` column that appears at file 009, and about 2% of events stamped roughly 25
minutes in the past. Every streaming exercise depends on at least one of them.

## Your workspace

Everything is namespaced by your username from `current_user()`.

| Object | Path |
| :-- | :-- |
| Schemas | `capstone_dev.<you>_bronze` / `_silver` / `_gold` |
| Raw files | `/Volumes/capstone_dev/<you>_bronze/raw_files` |
| Stream checkpoints | `/Volumes/capstone_dev/<you>_bronze/checkpoints` |
| Auto Loader schemas | `/Volumes/capstone_dev/<you>_bronze/schemas` |

## Streaming on serverless — three rules

1. **Always stop your queries.** A `processingTime` trigger runs until stopped. Every
   streaming notebook ends with `streaming_utils.stop_all_streams()` — run it.
2. **Prefer `availableNow`.** It processes what is there and stops. Use `processingTime`
   only when the exercise is about watching a stream iterate.
3. **A stream that does nothing is usually a stale checkpoint.** If you dropped a table but
   kept its checkpoint, the stream believes it has already consumed everything.
   `utils.reset_path(checkpoint_path)` clears it — drop the table at the same time.

## Deliverables

1. All ten notebooks, run, with outputs visible.
2. Every check passing in B01–B04 and S01–S04.
3. Written answers to the **Question:** prompts throughout, in markdown cells.
4. For B04 Step 4: the completed `batch_pipeline.job.yml` and a job run URL, or a
   screenshot of the equivalent job built in the UI.
5. The five wrap-up questions at the end of S04.

## Troubleshooting

**A streaming query fails with a schema error in S02** — that is Step 3, and it is meant to
happen. Restart the same query.

**A stream reports zero rows and finishes instantly** — stale checkpoint. See rule 3 above.

**`test_runner` reports it cannot find a module** — the name must match the notebook file
exactly, e.g. `test_runner.run("B01-batch-ingestion-and-delta")`.

**Checks reference the manifest and it is not found** — re-run `00-setup`.

**Everything is a mess and you want a clean slate** — run `99-cleanup` with `CONFIRM = True`,
then `00-setup` again. The replay files regenerate identically, so your earlier results stay
comparable.

## Cleanup

`99-cleanup.ipynb` stops your streams and drops your schemas, tables and volumes. Run it
only after grading.
