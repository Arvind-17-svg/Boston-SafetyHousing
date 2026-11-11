"""Poll Boston 311 (Open311) and publish new/changed service requests.

This is the one genuinely near-real-time public source in the project: the
API reflects updates within about a minute. It has no push mechanism, so
"streaming" here means a 60s poll with a watermark:

  1. GET requests updated after (watermark - overlap), newest first, paged.
  2. Normalize, hash, and drop rows whose hash we've already sent.
  3. Produce; after broker ack, advance per-record state and the watermark.

The watermark only advances to the max updated_at among *acked* records, so
a failed produce is retried on the next poll (at-least-once).

LABEL: this data is a reported-conditions signal (litter, potholes, parking,
needle pickups...). It reflects who reports, not just what happens, so it is
excluded from the safety score. Every event carries
signal_class="reported_condition" and safety_score_eligible=false.

Run continuously:  python -m ingestion.open311_producer
Single poll:       python -m ingestion.open311_producer --once
"""
from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Iterator

import requests

from .config import (
    DLQ_TOPIC,
    OPEN311_INITIAL_LOOKBACK_HOURS,
    OPEN311_OVERLAP_SECONDS,
    OPEN311_PAGE_SIZE,
    OPEN311_POLL_SECONDS,
    OPEN311_STATE_DB_PATH,
    OPEN311_URL,
    SERVICE_REQUEST_TOPIC,
)
from .kafka_utils import DeliveryTracker, encode, make_producer
from .schemas import ValidationError, normalize_service_request

log = logging.getLogger("open311_producer")
MAX_PAGES_PER_POLL = 50  # 15k records; a safety valve, never hit in steady state


# --- Source -----------------------------------------------------------------
def fetch_updated_since(session: requests.Session, since: datetime) -> Iterator[dict]:
    since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
    for page in range(1, MAX_PAGES_PER_POLL + 1):
        params = {"updated_after": since_str, "per_page": OPEN311_PAGE_SIZE, "page": page}
        rows = _get_with_retry(session, params)
        yield from rows
        if len(rows) < OPEN311_PAGE_SIZE:
            return
    log.warning("hit MAX_PAGES_PER_POLL; remaining rows picked up next poll")


def _get_with_retry(session: requests.Session, params: dict, attempts: int = 4) -> list[dict]:
    for i in range(attempts):
        try:
            r = session.get(OPEN311_URL, params=params, timeout=30)
            if r.status_code == 429:
                raise requests.HTTPError("rate limited (429)")
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, list):
                raise ValueError(f"unexpected payload: {str(data)[:200]}")
            return data
        except (requests.RequestException, ValueError) as e:
            if i == attempts - 1:
                raise
            wait = 2 ** (i + 1)
            log.warning("open311 request failed (%s); retrying in %ss", e, wait)
            time.sleep(wait)
    return []


# --- State --------------------------------------------------------------------
class Open311State:
    def __init__(self, path: str) -> None:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.executescript(
            "CREATE TABLE IF NOT EXISTS sent ("
            "  service_request_id TEXT PRIMARY KEY, content_hash TEXT NOT NULL);"
            "CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);"
        )

    def watermark(self) -> datetime | None:
        row = self.db.execute("SELECT v FROM meta WHERE k = 'watermark'").fetchone()
        return datetime.fromisoformat(row[0]) if row else None

    def sent_hash(self, srid: str) -> str | None:
        row = self.db.execute(
            "SELECT content_hash FROM sent WHERE service_request_id = ?", (srid,)
        ).fetchone()
        return row[0] if row else None

    def commit(self, pairs: list[tuple[str, str]], new_watermark: datetime | None) -> None:
        self.db.executemany(
            "INSERT INTO sent VALUES (?, ?) ON CONFLICT(service_request_id) "
            "DO UPDATE SET content_hash = excluded.content_hash",
            pairs,
        )
        if new_watermark is not None:
            self.db.execute(
                "INSERT INTO meta VALUES ('watermark', ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                (new_watermark.isoformat(),),
            )
        self.db.commit()


# --- Poll ---------------------------------------------------------------------
def poll_once(session: requests.Session, state: Open311State, producer, dry_run: bool) -> dict:
    wm = state.watermark()
    if wm is None:
        since = datetime.now(timezone.utc) - timedelta(hours=OPEN311_INITIAL_LOOKBACK_HOURS)
    else:
        since = wm - timedelta(seconds=OPEN311_OVERLAP_SECONDS)

    tracker = DeliveryTracker()
    pending: dict[str, tuple[str, datetime]] = {}
    stats = {"since": since.isoformat(), "read": 0, "unchanged": 0, "produced": 0, "dlq": 0}

    for raw in fetch_updated_since(session, since):
        stats["read"] += 1
        try:
            event = normalize_service_request(raw)
        except ValidationError as e:
            stats["dlq"] += 1
            if producer:
                safe_raw = {k: raw.get(k) for k in ("service_request_id", "service_name",
                                                     "requested_datetime", "updated_datetime")}
                producer.produce(DLQ_TOPIC, value=encode(
                    {"error": str(e), "raw": safe_raw, "source": "open311"}))
            continue
        key = event["service_request_id"]
        if state.sent_hash(key) == event["content_hash"] or (
            key in pending and pending[key][0] == event["content_hash"]
        ):
            stats["unchanged"] += 1
            continue
        pending[key] = (event["content_hash"], datetime.fromisoformat(event["updated_at_utc"]))
        stats["produced"] += 1
        if producer:
            producer.produce(SERVICE_REQUEST_TOPIC, key=key.encode(), value=encode(event),
                             on_delivery=tracker)
            producer.poll(0)

    if dry_run:
        log.info("dry run: %s", stats)
        return stats

    producer.flush(30)
    acked = [k for k in tracker.acked_keys if k in pending]
    # Advance the watermark only if every produce succeeded; otherwise keep
    # the old one so failed records are re-fetched next poll.
    new_wm = None
    if tracker.failed == 0:
        candidates = [pending[k][1] for k in acked]
        if wm:
            candidates.append(wm)
        new_wm = max(candidates) if candidates else None
    state.commit([(k, pending[k][0]) for k in acked], new_wm)
    stats.update(acked=tracker.ok, failed=tracker.failed,
                 watermark=(new_wm or wm).isoformat() if (new_wm or wm) else None)
    log.info("poll complete: %s", stats)
    return stats


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--once", action="store_true", help="single poll, then exit")
    p.add_argument("--interval", type=int, default=OPEN311_POLL_SECONDS)
    p.add_argument("--dry-run", action="store_true", help="fetch + validate, don't produce or save state")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    session = requests.Session()
    state = Open311State(OPEN311_STATE_DB_PATH)
    producer = None if args.dry_run else make_producer("open311-producer")
    while True:
        started = time.monotonic()
        try:
            stats = poll_once(session, state, producer, args.dry_run)
        except requests.RequestException as e:
            # Source outage shouldn't kill a long-running poller.
            log.error("poll failed, will retry next interval: %s", e)
            stats = {"failed": 1}
        if args.once or args.dry_run:
            return 1 if stats.get("failed") else 0
        time.sleep(max(0.0, args.interval - (time.monotonic() - started)))


if __name__ == "__main__":
    sys.exit(main())
