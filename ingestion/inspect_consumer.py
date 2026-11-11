"""Debug consumer: reads all three topics from the beginning and prints what a
downstream job would see. This is a *verification tool*, standing in for the
Spark Structured Streaming job until Phase 5 -- not part of the real pipeline.

It demonstrates the two consumption patterns:
  * crime-events   -> dedupe on incident_number (keep latest version), then
                      aggregate counts per geohash-6 cell
  * listing-events -> materialize current table (tombstones delete rows)
  * service-request-events -> latest version per request, reported SEPARATELY
                      as a labeled conditions signal (never mixed into crime)
and a naive listing x crime-cell join to show the data lines up.
"""
from __future__ import annotations

import argparse
import json
import time
import uuid
from collections import Counter

from confluent_kafka import Consumer, KafkaError

from .config import BOOTSTRAP_SERVERS, CRIME_TOPIC, DLQ_TOPIC, LISTING_TOPIC, SERVICE_REQUEST_TOPIC


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--idle-timeout", type=float, default=8.0, help="exit after N seconds w/o messages")
    args = p.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"inspect-{uuid.uuid4().hex[:8]}",  # fresh group => always from offset 0
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([CRIME_TOPIC, LISTING_TOPIC, SERVICE_REQUEST_TOPIC, DLQ_TOPIC])

    crimes: dict[str, dict] = {}
    crime_msgs = 0
    listings: dict[str, dict] = {}
    listing_msgs = tombstones = dlq = 0
    requests_: dict[str, dict] = {}
    sr_msgs = 0
    last_msg = time.time()

    while time.time() - last_msg < args.idle_timeout:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print("consumer error:", msg.error())
            continue
        last_msg = time.time()
        topic, key = msg.topic(), msg.key().decode() if msg.key() else None
        if topic == CRIME_TOPIC:
            crime_msgs += 1
            crimes[key] = json.loads(msg.value())  # later version wins
        elif topic == LISTING_TOPIC:
            listing_msgs += 1
            if msg.value() is None:
                tombstones += 1
                listings.pop(key, None)
            else:
                listings[key] = json.loads(msg.value())
        elif topic == SERVICE_REQUEST_TOPIC:
            sr_msgs += 1
            requests_[key] = json.loads(msg.value())
        else:
            dlq += 1
    consumer.close()

    located = [c for c in crimes.values() if c["has_location"]]
    cell_counts = Counter(c["geohash"][:6] for c in located)
    print(f"\n== {CRIME_TOPIC}: {crime_msgs} msgs -> {len(crimes)} unique incidents "
          f"({len(crimes) - len(located)} without location), {dlq} in DLQ")
    print("top geohash-6 cells:")
    for cell, n in cell_counts.most_common(8):
        print(f"  {cell}  {n}")

    print(f"\n== {LISTING_TOPIC}: {listing_msgs} msgs, {tombstones} tombstones "
          f"-> {len(listings)} live listings")
    sr_cells = Counter(r["geohash"][:6] for r in requests_.values()
                       if r["has_location"] and r["status"] == "open")
    cats = Counter(r["condition_category"] for r in requests_.values())
    print(f"\n== {SERVICE_REQUEST_TOPIC} [LABELED CONDITIONS SIGNAL - not in safety score]: "
          f"{sr_msgs} msgs -> {len(requests_)} unique requests")
    print("  by category:", ", ".join(f"{c}={n}" for c, n in cats.most_common()))

    sample = sorted(listings.values(), key=lambda l: -cell_counts.get(l["geohash"][:6], 0))[:5]
    print("listings in the busiest cells (naive preview of the Spark join):")
    for l in sample:
        print(f"  {l['listing_id']}  {l['neighborhood']:<14} ${l['price']:>5}  "
              f"cell={l['geohash'][:6]}  crimes_in_cell={cell_counts.get(l['geohash'][:6], 0)}  "
              f"| open_311_reports={sr_cells.get(l['geohash'][:6], 0)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
