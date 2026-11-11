"""Shared producer construction and delivery bookkeeping."""
from __future__ import annotations

import json
import logging

from confluent_kafka import Producer

from .config import BOOTSTRAP_SERVERS

log = logging.getLogger(__name__)


def make_producer(client_id: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": client_id,
            # Durability: wait for all in-sync replicas, and let the broker
            # dedupe retries (idempotent producer => no dupes from retries,
            # ordering preserved per partition).
            "acks": "all",
            "enable.idempotence": True,
            # Throughput: we're batch-shaped (thousands of rows per pull), so
            # trading a few ms of latency for bigger compressed batches is free.
            "linger.ms": 50,
            "compression.type": "lz4",
        }
    )


class DeliveryTracker:
    """Counts acks/failures from async delivery callbacks."""

    def __init__(self) -> None:
        self.ok = 0
        self.failed = 0
        self.acked_keys: list[str] = []

    def __call__(self, err, msg) -> None:
        if err is not None:
            self.failed += 1
            log.error("delivery failed key=%s: %s", msg.key(), err)
        else:
            self.ok += 1
            if msg.key() is not None:
                self.acked_keys.append(msg.key().decode())


def encode(value: dict | None) -> bytes | None:
    return None if value is None else json.dumps(value, separators=(",", ":")).encode()
