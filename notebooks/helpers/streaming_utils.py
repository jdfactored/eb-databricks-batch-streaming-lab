"""
Helpers for inspecting Structured Streaming queries.

`StreamingQueryListener` is not reliably available on serverless compute, so everything
here reads `query.recentProgress` / `query.lastProgress` instead. Those are plain dicts,
always available, and they contain the same numbers a listener would give you.
"""

import time
from typing import Dict, List, Optional

from pyspark.sql.streaming import StreamingQuery

from helpers import utils

spark = utils.spark


def stop_all_streams() -> None:
    """Stop every active query. Run this before re-running a streaming exercise."""
    for query in spark.streams.active:
        print(f"Stopping {query.name or query.id}")
        query.stop()
    print(f"{len(spark.streams.active)} query/queries still active.")


def await_batches(query: StreamingQuery, batches: int = 3, timeout_seconds: int = 180) -> None:
    """
    Block until a `processingTime` query has completed at least `batches` micro-batches.

    Use this instead of `awaitTermination()` for continuously-running queries, which
    would otherwise block forever and burn compute.
    """
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if len(query.recentProgress) >= batches:
            print(f"{len(query.recentProgress)} batches completed.")
            return
        if not query.isActive:
            print("Query stopped before reaching the requested batch count.")
            return
        time.sleep(2)
    print(f"Timed out after {timeout_seconds}s with {len(query.recentProgress)} batch(es).")


def progress_summary(query: StreamingQuery) -> List[Dict]:
    """
    One flat dict per completed micro-batch. These six numbers answer most
    "why is my stream slow" questions.
    """
    rows = []
    for p in query.recentProgress:
        source = (p.get("sources") or [{}])[0]
        rows.append({
            "batch_id": p.get("batchId"),
            "input_rows": p.get("numInputRows"),
            "rows_per_second": round(p.get("processedRowsPerSecond") or 0, 1),
            "batch_duration_ms": p.get("batchDuration"),
            "trigger_ms": (p.get("durationMs") or {}).get("triggerExecution"),
            "backlog_rows": source.get("numInputRows"),
        })
    return rows


def show_progress(query: StreamingQuery) -> None:
    rows = progress_summary(query)
    if not rows:
        print("No completed micro-batches yet.")
        return
    spark.createDataFrame(rows).orderBy("batch_id").display()


def state_summary(query: StreamingQuery) -> List[Dict]:
    """
    State store metrics per micro-batch. Only populated for stateful queries
    (aggregations, joins, dropDuplicates). An ever-growing `rows_total` on a windowed
    aggregation is the classic sign of a missing or too-generous watermark.
    """
    rows = []
    for p in query.recentProgress:
        for op in p.get("stateOperators") or []:
            rows.append({
                "batch_id": p.get("batchId"),
                "operator": op.get("operatorName"),
                "rows_total": op.get("numRowsTotal"),
                "rows_updated": op.get("numRowsUpdated"),
                "rows_removed": op.get("numRowsRemoved"),
                "memory_bytes": op.get("memoryUsedBytes"),
            })
    return rows


def watermark_of(query: StreamingQuery) -> Optional[str]:
    """The watermark the query had reached at its last micro-batch."""
    last = query.lastProgress
    if not last:
        return None
    return (last.get("eventTime") or {}).get("watermark")


def describe_query(query: StreamingQuery) -> None:
    """One-screen summary of a running query."""
    print(f"name:      {query.name}")
    print(f"id:        {query.id}")
    print(f"active:    {query.isActive}")
    print(f"batches:   {len(query.recentProgress)}")
    print(f"watermark: {watermark_of(query)}")
    if query.exception():
        print(f"exception: {query.exception()}")
