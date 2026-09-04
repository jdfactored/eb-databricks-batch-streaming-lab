import json
import uuid
import random
import time
import io
import os
from datetime import datetime, timezone
from typing import Dict, List

try:
    from . import utils
except ImportError:
    from helpers import utils

EVENT_TYPES = ["view", "click", "add_to_cart"]
ENDPOINTS: Dict[str, str] = {
    "view": "/products/view",
    "click": "/products/click",
    "add_to_cart": "/cart/add",
}
DEVICE_TYPES = ["mobile", "desktop", "tablet"]


def generate_event_id() -> str:
    """Generate a unique UUID for the event."""
    return str(uuid.uuid4())


def generate_session_id() -> str:
    """Generate a unique UUID for the user session."""
    return str(uuid.uuid4())


def generate_user_id() -> int:
    """Generate a random user ID between 1 and 50."""
    return random.randint(1001, 1050)


def generate_product_id() -> int:
    """Generate a random product ID between 1 and 50."""
    return random.randint(1, 50)


def generate_event_timestamp() -> str:
    """Return the current UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


def generate_device_type() -> str:
    """Randomly select a device type from the predefined list."""
    return random.choice(DEVICE_TYPES)


def generate_event() -> Dict:
    """
    Generate a single clickstream event with coherent fields.
    """
    event_type = random.choice(EVENT_TYPES)
    event: Dict = {
        "event_id": generate_event_id(),
        "user_id": generate_user_id(),
        "session_id": generate_session_id(),
        "event_timestamp": generate_event_timestamp(),
        "event_type": event_type,
        "device_type": generate_device_type(),
        "endpoint": ENDPOINTS[event_type],
    }

    if event_type in ["click", "add_to_cart"]:
        event["product_id"] = generate_product_id()
    else:
        event["product_id"] = None

    return event


def inject_missing_data(event: Dict, record_index: int, null_frequency: int) -> Dict:
    """
    Optionally inject missing values to simulate incomplete data.
    """
    if record_index % null_frequency == 0:
        event["user_id"] = None
    return event


def save_batch_to_buffer(events: List[Dict], batch_num: int) -> io.StringIO:
    """
    Serialize a batch of events into a JSON buffer.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    buffer = io.StringIO()
    json.dump(events, buffer, indent=4, ensure_ascii=False)
    buffer.seek(0)
    return buffer


def upload_batch_to_databricks(buffer: io.StringIO, batch_num: int, env: str) -> None:
    """
    Upload the JSON buffer to a Databricks volume using direct filesystem access.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    filename = f"stream_batch_{batch_num}_{timestamp}.json"
    volume_path = f"/Volumes/capstone_{env}/{utils.get_base_user_schema()}_bronze/raw_files/web_site_events"
    destination_path = f"{volume_path}/{filename}"

    os.makedirs(volume_path, exist_ok=True)
    with open(destination_path, "w", encoding="utf-8") as f:
        f.write(buffer.getvalue())
    print(f"[UPLOAD] {destination_path}")


def stream_events(batch_size: int, delay_seconds: float, total_batches: int,
                  null_frequency: int, env: str, enable_schema_evolution: bool = False) -> None:
    """
    Generate clickstream events in micro-batches, optionally inserting missing data,
    and upload each batch to Databricks Volume as a JSON file.
    """
    total_generated = 0

    for batch_num in range(1, total_batches + 1):
        events: List[Dict] = []

        # Generate events for this batch
        for _ in range(batch_size):
            total_generated += 1
            event = generate_event()
            event = inject_missing_data(event, total_generated, null_frequency)

            # Optional: add new field for schema evolution testing
            if enable_schema_evolution:
                event["referrer_url"] = random.choice([
                    "https://google.com",
                    "https://facebook.com",
                    "https://instagram.com",
                    "https://twitter.com",
                    "https://linkedin.com",
                    None  # simulate missing referrer
                ])

            events.append(event)

        # Save to buffer & upload
        buffer = save_batch_to_buffer(events, batch_num)
        upload_batch_to_databricks(buffer, batch_num, env)

        print(
            f"[STREAM] Batch {batch_num}/{total_batches} uploaded "
            f"({len(events)} events, {delay_seconds}s delay)"
        )

        time.sleep(delay_seconds)


if __name__ == "__main__":
    # Example: 10,000 events per batch, 10 batches, 5s delay, null every 500 events
    stream_events(
        batch_size=1000,
        delay_seconds=5,
        total_batches=10,
        null_frequency=500,
        env="dev",
        enable_schema_evolution=True
    )
