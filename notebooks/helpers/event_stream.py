"""
Clickstream event sources for the streaming track — no Kafka, no external broker.

Four sources, each with a different teaching job:

  1. rate_events()      Spark's built-in `rate` source, mapped into realistic events.
                        No files, no volume, no setup. Use it to teach triggers,
                        output modes and checkpoints with nothing else to go wrong.

  2. write_replay_files()  Pre-generates a fixed set of JSON files ONCE. Read them with
                        Auto Loader and `cloudFiles.maxFilesPerTrigger = 1` and you get
                        exactly one micro-batch per file, identical on every run. This
                        is what makes the streaming exercises gradeable.

  3. start_drip()       Writes one file every N seconds on a background thread while a
                        `processingTime` stream is already running. The only source that
                        feels genuinely live — and the only one that is not deterministic.

  4. (no code needed)   A Delta table read with `spark.readStream.table(...)`. Versions
                        act as offsets, so it replays like a log broker. Covered in S03.

Everything generated here is deterministic: the same file index always produces the
same events, byte for byte, because each file seeds its own `random.Random`.

The data deliberately contains three defects, because the exercises need them:

  * Duplicates      — each file after the first repeats DUPES_PER_FILE events from the
                      previous file, so `dropDuplicatesWithinWatermark` has work to do.
  * Late arrivals   — LATE_RATE of events carry a timestamp LATE_MINUTES in the past, so
                      a watermark actually drops something and the metric is non-zero.
  * Schema drift    — `referrer_url` appears only from file SCHEMA_DRIFT_FILE onwards,
                      so Auto Loader's `addNewColumns` mode triggers for real.
"""

import io
import json
import random
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterator, List, Optional

from pyspark.sql import DataFrame, functions as F

from helpers import utils

spark = utils.spark

# --------------------------------------------------------------------------- shape

EVENT_TYPES = ["view", "click", "add_to_cart", "purchase"]
EVENT_WEIGHTS = [0.55, 0.25, 0.13, 0.07]
ENDPOINTS = {
    "view": "/products/view",
    "click": "/products/click",
    "add_to_cart": "/cart/add",
    "purchase": "/checkout/complete",
}
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
REFERRERS = ["/home", "/search", "/category/electronics", "/promo/spring", None]

USER_ID_MIN, USER_ID_MAX = 1001, 1050
PRODUCT_ID_MIN, PRODUCT_ID_MAX = 1, 50

# --------------------------------------------------------------------------- replay config
# Change these and the manifest changes with them — the checks read the manifest,
# they never hardcode counts.

REPLAY_FILES = 12
EVENTS_PER_FILE = 500
DUPES_PER_FILE = 15          # repeated from the previous file
LATE_RATE = 0.02             # fraction of events stamped in the past
LATE_MINUTES = 25
SCHEMA_DRIFT_FILE = 9        # referrer_url appears from this file onwards (1-based)
MINUTES_PER_FILE = 5

# Fixed base time so windowed aggregations are reproducible across runs and cohorts.
BASE_TIME = datetime(2026, 3, 2, 8, 0, 0, tzinfo=timezone.utc)

REPLAY_DIR = "web_events_replay"
DRIP_DIR = "web_events_live"
META_DIR = "_meta"
STAGED_DIR = "web_events_staged"


def staged_path() -> str:
    return f"{utils.get_configs()['volume_raw']}/{STAGED_DIR}"

# --------------------------------------------------------------------------- generation

def _event_id(file_index: int, n: int) -> str:
    """Stable UUID derived from position, so reruns produce identical ids."""
    return str(uuid.UUID(int=(file_index * 1_000_003 + n) * 2_654_435_761 % (1 << 128)))


def _make_event(rng: random.Random, file_index: int, n: int, with_referrer: bool) -> Dict:
    event_type = rng.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

    offset = timedelta(
        minutes=(file_index - 1) * MINUTES_PER_FILE,
        seconds=rng.randint(0, MINUTES_PER_FILE * 60 - 1),
    )
    is_late = rng.random() < LATE_RATE
    if is_late:
        offset -= timedelta(minutes=LATE_MINUTES + rng.randint(0, 10))

    event = {
        "event_id": _event_id(file_index, n),
        "user_id": rng.randint(USER_ID_MIN, USER_ID_MAX),
        "session_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "event_timestamp": (BASE_TIME + offset).isoformat(),
        "event_type": event_type,
        "device_type": rng.choice(DEVICE_TYPES),
        "endpoint": ENDPOINTS[event_type],
        "product_id": None if event_type == "view" else rng.randint(PRODUCT_ID_MIN, PRODUCT_ID_MAX),
    }
    if with_referrer:
        event["referrer_url"] = rng.choice(REFERRERS)
    return event


def generate_file_events(file_index: int) -> List[Dict]:
    """
    Deterministic event list for one replay file.

    Files after the first repeat DUPES_PER_FILE events from the previous file, so the
    same event_id legitimately appears twice in the stream.
    """
    rng = random.Random(f"factored-de013-file-{file_index}")
    with_referrer = file_index >= SCHEMA_DRIFT_FILE

    events = [_make_event(rng, file_index, n, with_referrer) for n in range(EVENTS_PER_FILE)]

    if file_index > 1 and DUPES_PER_FILE > 0:
        previous = generate_file_events(file_index - 1)
        dupes = previous[:DUPES_PER_FILE]
        if with_referrer:
            for d in dupes:
                d.setdefault("referrer_url", None)
        else:
            dupes = [{k: v for k, v in d.items() if k != "referrer_url"} for d in dupes]
        events[:DUPES_PER_FILE] = dupes

    return events


# --------------------------------------------------------------------------- writing

def _upload_json(path: str, payload: List[Dict]) -> None:
    """Write newline-delimited JSON to a UC volume path."""
    body = "\n".join(json.dumps(e) for e in payload).encode("utf-8")
    utils.workspace.files.upload(path, io.BytesIO(body), overwrite=True)


def replay_path() -> str:
    return f"{utils.get_configs()['volume_raw']}/{REPLAY_DIR}"

def reset_replay_directory():
    """Clears replay_path and copies only files 001-008."""
    target = replay_path()
    staged = staged_path()
    
    # Clean target directory
    if utils.path_exists(target):
        for f in utils.list_files(target):
            utils.workspace.files.delete(f.path)

    # Copy files 001 to 008 (pre-schema drift)
    for i in range(1, SCHEMA_DRIFT_FILE):
        filename = f"events_{i:03d}.json"
        body = utils.workspace.files.download(f"{staged}/{filename}").contents.read()
        utils.workspace.files.upload(f"{target}/{filename}", io.BytesIO(body), overwrite=True)
    print(f"Replay path prepared with initial files 001-{SCHEMA_DRIFT_FILE-1:03d}.")

def land_drift_files():
    """Simulates new data landing mid-stream by copying files 009-012 into replay_path."""
    target = replay_path()
    staged = staged_path()
    for i in range(SCHEMA_DRIFT_FILE, REPLAY_FILES + 1):
        filename = f"events_{i:03d}.json"
        body = utils.workspace.files.download(f"{staged}/{filename}").contents.read()
        utils.workspace.files.upload(f"{target}/{filename}", io.BytesIO(body), overwrite=True)
    print(f"Landed drift files {SCHEMA_DRIFT_FILE:03d}-{REPLAY_FILES:03d} into {target}.")

def drip_path() -> str:
    return f"{utils.get_configs()['volume_raw']}/{DRIP_DIR}"


def manifest_path() -> str:
    return f"{utils.get_configs()['volume_raw']}/{META_DIR}/replay_manifest.json"


def build_manifest() -> Dict:
    """Ground truth for the checks, computed from the generator itself."""
    all_events, distinct_ids, late = [], set(), 0
    for i in range(1, REPLAY_FILES + 1):
        events = generate_file_events(i)
        all_events.extend(events)
        distinct_ids.update(e["event_id"] for e in events)

    for e in all_events:
        ts = datetime.fromisoformat(e["event_timestamp"])
        expected_floor = BASE_TIME  # anything before base is definitionally late
        if ts < expected_floor:
            late += 1

    return {
        "files": REPLAY_FILES,
        "events_per_file": EVENTS_PER_FILE,
        "total_events": len(all_events),
        "distinct_event_ids": len(distinct_ids),
        "duplicate_events": len(all_events) - len(distinct_ids),
        "events_before_base_time": late,
        "schema_drift_file": SCHEMA_DRIFT_FILE,
        "base_time": BASE_TIME.isoformat(),
        "minutes_per_file": MINUTES_PER_FILE,
    }


def write_replay_files(force: bool = False) -> Dict:
    """Generates all 12 files into staging and initializes replay_path with files 001-008."""
    staged = staged_path()

    if not force and utils.path_exists(staged) and len(utils.list_files(staged)) == REPLAY_FILES:
        print(f"Replay files already present at {staged} — skipping.")
    else:
        for i in range(1, REPLAY_FILES + 1):
            events = generate_file_events(i)
            _upload_json(f"{staged}/events_{i:03d}.json", events)
            print(f"  wrote events_{i:03d}.json  ({len(events)} events)")

    # 2. Populate replay directory with files 001-008 only
    reset_replay_directory()

    manifest = build_manifest()
    utils.workspace.files.upload(
        manifest_path(), io.BytesIO(json.dumps(manifest, indent=2).encode("utf-8")), overwrite=True
    )

    print(f"\n{manifest['total_events']} events across {manifest['files']} files")
    print(f"{manifest['distinct_event_ids']} distinct event_ids "
          f"({manifest['duplicate_events']} duplicates)")
    print(f"referrer_url appears from file {manifest['schema_drift_file']:03d} onwards")

    return manifest


def read_manifest() -> Dict:
    raw = utils.workspace.files.download(manifest_path()).contents.read()
    return json.loads(raw.decode("utf-8"))


# --------------------------------------------------------------------------- source 3: drip

def start_drip(num_files: int = 10, events_per_file: int = 200,
               delay_seconds: float = 5.0) -> threading.Thread:
    """
    Write one JSON file every `delay_seconds` on a background thread.

    Start this, then immediately start a `processingTime` stream in the next cell and
    watch files land while the query is running. Non-deterministic on purpose — this is
    the source you use to *see* a stream behave, not the one you grade.

    Returns the thread so you can `join()` it.
    """
    target = drip_path()
    run_id = datetime.now(timezone.utc).strftime("%H%M%S")

    def _worker():
        for i in range(1, num_files + 1):
            rng = random.Random(f"drip-{run_id}-{i}")
            now = datetime.now(timezone.utc)
            events = []
            for n in range(events_per_file):
                e = _make_event(rng, i, n, with_referrer=True)
                e["event_timestamp"] = (now - timedelta(seconds=rng.randint(0, 30))).isoformat()
                e["event_id"] = str(uuid.uuid4())
                events.append(e)
            _upload_json(f"{target}/live_{run_id}_{i:03d}.json", events)
            print(f"[drip] live_{run_id}_{i:03d}.json  ({events_per_file} events)")
            time.sleep(delay_seconds)
        print("[drip] finished")

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()
    print(f"Drip started: {num_files} files, one every {delay_seconds}s, into {target}")
    return thread


# --------------------------------------------------------------------------- source 1: rate

def rate_events(rows_per_second: int = 20) -> DataFrame:
    """
    Spark's built-in `rate` source, projected into the same event shape as the files.

    Every column is a deterministic function of the rate source's `value`, so row 1_000
    always describes the same user and product no matter when you run it. There are no
    files, no volume and no checkpoint content to clean up between attempts, which makes
    this the right source for teaching trigger and output-mode mechanics.
    """
    v = F.col("value")

    return (
        spark.readStream.format("rate")
        .option("rowsPerSecond", rows_per_second)
        .load()
        .select(
            F.concat(F.lit("rate-"), F.lpad(v.cast("string"), 10, "0")).alias("event_id"),
            (F.lit(USER_ID_MIN) + F.pmod(v * F.lit(7919), F.lit(USER_ID_MAX - USER_ID_MIN + 1)))
                .cast("int").alias("user_id"),
            F.col("timestamp").alias("event_timestamp"),
            F.element_at(
                F.array(*[F.lit(t) for t in EVENT_TYPES]),
                (F.pmod(v * F.lit(2_654_435_761), F.lit(len(EVENT_TYPES))) + 1).cast("int"),
            ).alias("event_type"),
            F.element_at(
                F.array(*[F.lit(d) for d in DEVICE_TYPES]),
                (F.pmod(v * F.lit(40_503), F.lit(len(DEVICE_TYPES))) + 1).cast("int"),
            ).alias("device_type"),
            (F.lit(PRODUCT_ID_MIN) + F.pmod(v * F.lit(97), F.lit(PRODUCT_ID_MAX)))
                .cast("int").alias("product_id"),
        )
    )
