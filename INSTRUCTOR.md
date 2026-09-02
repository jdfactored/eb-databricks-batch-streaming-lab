# Instructor Guide

Companion to `README.md` (student-facing) and `SOLUTIONS.md` (keep private).

---

## Before the cohort starts

**1. Seed the shared source catalog.** The batch track copies from a read-only catalog:

```
/Volumes/capstone_src/dev_bronze/raw_files/products/products_*.csv
/Volumes/capstone_src/dev_bronze/raw_files/users/users_*.csv
/Volumes/capstone_src/dev_bronze/raw_files/sales/sales_*.csv
```

These are the same files as the original capstone. If the catalog moved, change
`SOURCE_CATALOG` / `SOURCE_SCHEMA` in `helpers/utils.py` — the only place they appear.

**2. Verify the sales feed has what B01 needs.** Check before the cohort starts, because
several checks depend on it:

- Multiple daily files, day 0 plus deltas.
- A `status` column with `insert`, `update` **and** `delete` values.
- At least one `delete` for a `sale_id` that was previously inserted.
- A `region` column that appears partway through the series, not in file 1.

`test_deletes_became_soft_deletes` fails with a clear message if the deletes are missing, but
better to catch it yourself.

**3. The streaming data needs no seeding.** It is generated in `00-setup` from
`helpers/event_stream.py`, deterministically. Nothing to provision, nothing to go stale. If
you change `REPLAY_FILES` or `EVENTS_PER_FILE`, the manifest changes with them and the checks
follow — they read the manifest, never a hardcoded number.

**4. Grants.** On `capstone_dev`: `USE CATALOG`, `CREATE SCHEMA`. On `capstone_src`:
`USE CATALOG`, `USE SCHEMA`, `READ VOLUME`.

**5. Decide about `pii_readers`.** B02 Step 5 and S04 Step 4 call
`is_account_group_member('pii_readers')`. The function returns false for a group that does not
exist, so the exercises work either way — students see the masked branch. If you want somebody
to see the unmasked branch, create the group and add one student, then have them compare
outputs in class. That five-minute demo lands the concept far better than the notebook text.

**6. Serverless and streaming.** Everything is written for serverless. Every exercise that can
use `trigger(availableNow=True)` does. Two places use `processingTime` because watching a
stream iterate is the point — S01 Step 3 and S03 Step 4 — and both are bounded by
`streaming_utils.await_batches()` with a timeout rather than `awaitTermination()`.

If your workspace turns out to restrict something on serverless, the fallback is a
single-node classic cluster; nothing in the lab requires more than one node. Do a dry run
before the cohort.

**7. Cost.** The main risk is a student leaving a `processingTime` query running overnight.
`test_no_streams_left_running` in S04 catches it at the end of the track, and `99-cleanup`
stops everything, but consider a workspace-level idle policy as a backstop.

---

## Timing

| Notebook | Target | Where students lose time |
| :-- | :-- | :-- |
| `00-setup` | 5 min | Nothing, unless grants are wrong |
| `B01` | 75 min | Step 5, MERGE clause ordering |
| `B02` | 60 min | Step 1, building `dq_failed_rules` as an array |
| `B03` | 60 min | Step 4, reading the plan output |
| `B04` | 75 min | Step 1, SCD2 interval arithmetic |
| `S01` | 50 min | Step 5, understanding what restart actually did |
| `S02` | 70 min | Step 3, the deliberate failure |
| `S03` | 75 min | Step 3, `foreachBatch` with a null-safe merge key |
| `S04` | 65 min | Step 2, forgetting to use fresh checkpoints per run |

About 9 hours. Comfortable over two weeks part-time.

---

## Known sticking points

**B01 Step 5 — MERGE clause order.** `WHEN MATCHED AND status = 'delete'` must come before the
general `WHEN MATCHED`. First match wins, so the general clause swallows the deletes and
`_is_active` never goes false. The check message points at it but does not explain why. This
is the single best whole-room teaching moment in the batch track.

**B01 Step 5 — multiple source rows.** Students who skip the `ROW_NUMBER` dedup get
`Cannot perform Merge as multiple source rows matched`. Let them hit it.

**B02 Step 1 — `array_compact`.** Needs DBR 14.3+. On older runtimes:
`F.expr("filter(array(...), x -> x IS NOT NULL)")`. Mention it if your workspace runs older.

**B03 Step 2 — timing noise.** The second run of any query is faster. Students conclude their
change worked when they have measured cache warmup. The notebook asks about it directly;
reinforce that `files_scanned` is the trustworthy number and `duration_ms` is context.

**S02 Step 2 — the deliberate failure.** The stream stops with `UnknownFieldException` at file
009. The notebook says it will happen, in bold, before the cell. Students will still ask.
Consider mentioning it in the live session so the first person to hit it does not lose ten
minutes.

**S03 Step 2 — inner vs left join.** More than half the events are `view` with a null
`product_id`. An inner join deletes them and the check catches it, but the message is worth
walking through because the same mistake in production is silent.

**S03 Step 3 — the null-safe merge key.** `ON t.product_id = s.product_id` never matches null
product ids, so every batch inserts a fresh null row and the funnel accumulates duplicates.
`<=>` fixes it. This one is genuinely hard to spot; be ready to hint.

**S04 Step 2 — shared checkpoints.** Both tuning runs must have their own checkpoint, schema
location and target table. Reusing one means run B finds nothing to consume and reports 0
rows. The check that both runs saw the same row count catches it.

**Stale checkpoints generally.** The most common "my stream does nothing" cause across the
whole streaming track. `utils.reset_path(checkpoint)` plus dropping the table is the fix, and
it is worth saying out loud in the first session.

---

## Grading rubric

Out of 100. Batch and streaming weighted equally.

| Component | Points | Looking for |
| :-- | --: | :-- |
| Checks pass, batch track | 20 | 5 per notebook |
| Checks pass, streaming track | 20 | 5 per notebook |
| Code quality | 10 | Uses `utils.get_configs()`; no hardcoded names; loops instead of copy-paste; streams always stopped |
| B01–B02 reflections | 10 | The idempotency source; where masking actually fails (nothing stops them querying silver) |
| B03–B04 reflections | 10 | Measurement discipline; the SCD2 overlap consequence |
| S01–S02 reflections | 10 | What the checkpoint stores; why `addNewColumns` stopping is correct |
| S03–S04 reflections | 10 | The watermark trade-off; `foreachBatch` at-least-once |
| B04 Step 4 job | 5 | Working job graph with correct dependencies |
| Wrap-up questions | 5 | Q3 and Q5 are the discriminating ones |

**Auto-fail:** notebooks submitted without outputs; a streaming query left active at
submission time.

**Be generous with:** anyone who solves S03 Step 3 with a proper `batch_id` idempotency guard
rather than the plain accumulation — that is beyond what was asked and shows they understood
the failure mode. Also anyone who writes the B02 masking answer honestly, i.e. "the view is
pointless unless you revoke `SELECT` on silver". That is the correct and slightly
uncomfortable answer.

**Dock code quality, not correctness, for:** partitioning where clustering was taught,
hardcoded table names, and `awaitTermination()` on a `processingTime` query (works, but it is
how students accidentally leave streams running).

---

## Reuse from the original capstone

The monolith (`eb-databricks-for-de-capstone-homework`) covered all of this in one two-week
project. This lab keeps its data and its structural patterns and rebuilds the exercises.

| Old asset | Disposition |
| :-- | :-- |
| Seed CSVs in `capstone_src` | Reused unchanged for the batch track |
| Per-user schema isolation (`utils.py`) | Reused, extended with gold and checkpoint paths |
| `test_runner` pattern | Reused; notebook name now passed explicitly, prefix regex handles `B01-`/`S01-` |
| `1-batch-ingest-copy-into` | Folded into B01 Step 1, now with `_metadata` capture |
| `3-cdc-merge` | Became B01 Steps 4–5, with soft deletes and per-day replay |
| `2-streaming-autoloader` | Rebuilt as S02 on deterministic replay files; the old `self` bug is gone |
| `generate_streaming_events.py` | Rewritten as `event_stream.py` — deterministic, manifest-backed, three sources instead of one |
| `4-transformations-dlt`, `5-gold-queries-and-mvs` | Not carried over — DLT/Lakeflow is a separate course |
| `project_bundle/` | Rebuilt for B04 with one worked task instead of an entirely blank file |

**Bugs from the old repo that are fixed here** (worth knowing if you reuse anything else):
stray `self` on module-level functions in the streaming notebook; six `#TO DO` function bodies
with no `pass`, making notebook 5 unrunnable; markdown documenting functions that no longer
existed; `import dlt` and `pyspark.pipelines` mixed in one lab; checksum tests against an
unmaterialised source table; `%pip install pytest` that was never used; `test_runner` scraping
a notebook name via an API unavailable on serverless.

**The streaming generator is the biggest change.** The old one produced random events, so no
check could assert anything precise — the tests could only say "more than zero rows". The new
one is seeded per file, so 6,000 events with 165 duplicates and a schema change at file 009
happen identically for every student, every run. That is what makes exact-count assertions
possible, and exact counts are what turn vague streaming exercises into gradeable ones.

---

## Extending this lab

In rough order of value:

1. **Delta Change Data Feed.** Enable `delta.enableChangeDataFeed` on silver sales and have
   students stream the changes into gold with `readChangeFeed`. It is a natural extension of
   S03 Step 0 and it closes the loop between the two tracks.
2. **A failure injection exercise.** Corrupt one replay file mid-run and have students
   diagnose it from the progress metrics and rescued data alone, without being told what broke.
   Half an hour, and it is the closest thing to on-call practice the lab can offer.
3. **Lakeflow Declarative Pipelines.** The obvious next course. B01 and S02 map almost
   directly onto declarative table definitions with expectations, which makes a good bridge
   exercise if the two courses run back to back.
4. **The live drip generator.** `event_stream.start_drip()` exists and is documented but no
   notebook requires it. A 20-minute optional appendix — run the drip, watch a `processingTime`
   stream pick up files as they land — is the moment streaming stops being abstract for most
   people. Worth adding if your cohort has the time.
