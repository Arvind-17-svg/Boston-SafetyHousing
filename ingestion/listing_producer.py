"""Synthetic listing event generator.

Listings are modeled as a *table* on a compacted topic:
  key   = listing_id
  value = full current listing state (JSON), or null (tombstone) when delisted

Why full state instead of deltas ("price changed by -$100"): after compaction
only the last message per key survives, so a delta would be meaningless
without its predecessors. Full-state messages let any consumer rebuild the
current listing set by reading the topic from offset 0.

Addresses use real Boston street names within real neighborhoods; prices,
unit numbers and the listings themselves are synthetic (see Context.md,
tradeoff #3).
"""
from __future__ import annotations

import argparse
import logging
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from .config import LISTING_TOPIC
from .kafka_utils import DeliveryTracker, encode, make_producer
from .schemas import validate_listing_state

log = logging.getLogger("listing_producer")

# (neighborhood, approx centroid lat, lon, real streets, median 2BR rent)
NEIGHBORHOODS = [
    ("Back Bay", 42.3503, -71.0810, ["Commonwealth Ave", "Marlborough St", "Beacon St", "Newbury St"], 4200),
    ("South End", 42.3388, -71.0765, ["Tremont St", "Columbus Ave", "Shawmut Ave", "Washington St"], 3900),
    ("Jamaica Plain", 42.3097, -71.1151, ["Centre St", "South St", "Green St", "Lamartine St"], 3100),
    ("Dorchester", 42.3016, -71.0676, ["Dorchester Ave", "Adams St", "Bowdoin St", "Savin Hill Ave"], 2700),
    ("South Boston", 42.3381, -71.0476, ["E Broadway", "W Broadway", "E 4th St", "L St"], 3600),
    ("Allston", 42.3539, -71.1337, ["Harvard Ave", "Brighton Ave", "Commonwealth Ave", "Glenville Ave"], 2900),
    ("East Boston", 42.3702, -71.0389, ["Meridian St", "Bennington St", "Saratoga St", "Maverick St"], 2800),
    ("Roxbury", 42.3152, -71.0914, ["Blue Hill Ave", "Warren St", "Dudley St", "Columbus Ave"], 2500),
    ("Charlestown", 42.3782, -71.0602, ["Bunker Hill St", "Main St", "Medford St", "Warren St"], 3700),
    ("Mattapan", 42.2771, -71.0914, ["Blue Hill Ave", "Morton St", "River St", "Cummins Hwy"], 2400),
]


def _new_listing(rng: random.Random) -> dict:
    name, lat, lon, streets, median = rng.choice(NEIGHBORHOODS)
    beds = rng.choice([0, 1, 1, 2, 2, 2, 3, 3, 4])
    price = int(median * (0.55 + 0.25 * beds) * rng.uniform(0.85, 1.2) / 25) * 25
    return {
        "listing_id": f"lst_{uuid.UUID(int=rng.getrandbits(128)).hex[:12]}",
        "address": f"{rng.randint(1, 400)} {rng.choice(streets)}, Boston, MA",
        "neighborhood": name,
        # ~±400m jitter around the centroid
        "lat": round(lat + rng.uniform(-0.004, 0.004), 6),
        "lon": round(lon + rng.uniform(-0.005, 0.005), 6),
        "bedrooms": beds,
        "bathrooms": max(1, beds // 2 + rng.choice([0, 0, 1])),
        "price": price,
        "status": "active",
    }


def _stamp(state: dict, event_type: str) -> dict:
    s = dict(state)
    s["event_type"] = event_type
    s["updated_at"] = datetime.now(timezone.utc).isoformat()
    return s


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--initial", type=int, default=200, help="listings to seed")
    p.add_argument("--events", type=int, default=300, help="update events after seeding (0 = forever)")
    p.add_argument("--rate", type=float, default=20.0, help="events per second")
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    rng = random.Random(args.seed)
    producer = make_producer("listing-producer")
    tracker = DeliveryTracker()
    live: dict[str, dict] = {}
    counts = {"created": 0, "price_changed": 0, "status_changed": 0, "delisted": 0}

    def send(listing_id: str, state: dict | None) -> None:
        value = None if state is None else validate_listing_state(state)
        producer.produce(LISTING_TOPIC, key=listing_id.encode(), value=encode(value), on_delivery=tracker)
        producer.poll(0)

    for _ in range(args.initial):
        lst = _new_listing(rng)
        live[lst["listing_id"]] = lst
        send(lst["listing_id"], _stamp(lst, "listing_created"))
        counts["created"] += 1
    log.info("seeded %d listings", args.initial)

    i = 0
    while args.events == 0 or i < args.events:
        i += 1
        roll = rng.random()
        if roll < 0.35 or not live:
            lst = _new_listing(rng)
            live[lst["listing_id"]] = lst
            send(lst["listing_id"], _stamp(lst, "listing_created"))
            counts["created"] += 1
        elif roll < 0.75:
            lid = rng.choice(list(live))
            live[lid]["price"] = int(live[lid]["price"] * rng.uniform(0.93, 1.03) / 25) * 25
            send(lid, _stamp(live[lid], "listing_price_changed"))
            counts["price_changed"] += 1
        elif roll < 0.85:
            lid = rng.choice(list(live))
            live[lid]["status"] = "pending" if live[lid]["status"] == "active" else "active"
            send(lid, _stamp(live[lid], "listing_status_changed"))
            counts["status_changed"] += 1
        else:
            lid = rng.choice(list(live))
            del live[lid]
            send(lid, None)  # tombstone
            counts["delisted"] += 1
        time.sleep(1.0 / args.rate)

    producer.flush(30)
    log.info("done: %s live=%d acked=%d failed=%d", counts, len(live), tracker.ok, tracker.failed)
    return 1 if tracker.failed else 0


if __name__ == "__main__":
    sys.exit(main())
