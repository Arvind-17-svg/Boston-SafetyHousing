"""Create topics with explicit, versioned configuration. Idempotent."""
from __future__ import annotations

import logging
import sys

from confluent_kafka.admin import AdminClient, NewTopic

from .config import (
    BOOTSTRAP_SERVERS,
    CRIME_TOPIC,
    DLQ_TOPIC,
    LISTING_TOPIC,
    SERVICE_REQUEST_TOPIC,
)

DAY_MS = 24 * 60 * 60 * 1000

TOPICS = [
    # crime-events: an *upsert* stream keyed by incident_number (an incident
    # can be re-emitted when geocoding is backfilled). Kafka is the transport,
    # not the system of record -- the data lake raw zone is -- so time-based
    # retention is enough. 90 days lets a consumer be reset and replayed
    # through a full quarter when scoring/feature logic changes. At ~200
    # events/day that's ~18k small messages: storage is a non-issue.
    NewTopic(
        CRIME_TOPIC,
        num_partitions=3,
        replication_factor=1,
        config={"cleanup.policy": "delete", "retention.ms": str(90 * DAY_MS)},
    ),
    # service-request-events: Boston 311, a LABELED CONDITIONS SIGNAL that is
    # explicitly excluded from the safety score. Upsert stream keyed by
    # service_request_id (open -> closed transitions arrive as new versions).
    # Volume is ~1-2k events/day, the only near-real-time public source here.
    NewTopic(
        SERVICE_REQUEST_TOPIC,
        num_partitions=3,
        replication_factor=1,
        config={"cleanup.policy": "delete", "retention.ms": str(30 * DAY_MS)},
    ),
    # listing-events: a *table* of current listings keyed by listing_id.
    # Compaction keeps only the latest state per listing; a tombstone (null
    # value) deletes it. A new consumer can rebuild current state by reading
    # from the beginning without the topic growing forever.
    NewTopic(
        LISTING_TOPIC,
        num_partitions=3,
        replication_factor=1,
        config={
            "cleanup.policy": "compact",
            "min.cleanable.dirty.ratio": "0.1",
            "segment.ms": str(60 * 60 * 1000),
            "delete.retention.ms": str(DAY_MS),
        },
    ),
    # DLQ: records that failed validation, with the error attached. Long
    # retention because these are for humans to inspect.
    NewTopic(
        DLQ_TOPIC,
        num_partitions=1,
        replication_factor=1,
        config={"cleanup.policy": "delete", "retention.ms": str(30 * DAY_MS)},
    ),
]


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    existing = set(admin.list_topics(timeout=10).topics)
    to_create = [t for t in TOPICS if t.topic not in existing]
    for t in TOPICS:
        if t.topic in existing:
            logging.info("topic exists: %s", t.topic)
    if not to_create:
        return 0
    for name, fut in admin.create_topics(to_create).items():
        try:
            fut.result()
            logging.info("created topic: %s", name)
        except Exception as e:  # noqa: BLE001
            logging.error("failed to create %s: %s", name, e)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
