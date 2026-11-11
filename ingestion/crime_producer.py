"""Pull Boston crime incidents and publish new/changed ones to Kafka.

Shape of the problem: the source is a daily-refreshed table, not an event
stream. It is truncated and reloaded (so `_id` is not a stable cursor), and
old rows get *updated* (late geocoding). So this is change-data-capture by
diffing:

  1. Re-pull a trailing window (default 30 days by OCCURRED_ON_DATE).
  2. Normalize each row and hash its business fields.
  3. Produce only rows whose hash differs from what we last sent.
  4. Record the new hash ONLY after the broker acks the message.

Step 4 makes this at-least-once: if we crash between ack and state write,
the next run re-sends those rows. Consumers must therefore treat
(incident_number, content_hash) as idempotent -- which they need to anyway,
because an incident can legitimately appear more than once (as an update).

Run once (cron/backfill): python -m ingestion.crime_producer
Pull on publish:          python -m ingestion.crime_producer --watch 600

--watch polls the dataset's CKAN `last_modified` every N seconds (a tiny
metadata call) and does a full pull only when the city publishes a new
version. That removes *our* latency (minutes after publish instead of up to
a day), but the data still lands ~2-3 days after incidents occur because
that's when BPD publishes it. Crime is not, and cannot be made, real-time.
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
    CKAN_RESOURCE_SHOW_URL,
    CKAN_SQL_URL,
    DLQ_TOPIC,
    CRIME_LOOKBACK_DAYS,
    CRIME_PAGE_SIZE,
    CRIME_RESOURCE_ID,
    CRIME_TOPIC,
    STATE_DB_PATH,
)
from .kafka_utils import DeliveryTracker, encode, make_producer
from .schemas import BOSTON_TZ, ValidationError, normalize_crime

log = logging.getLogger("crime_producer")


# --- Source -----------------------------------------------------------------
def fetch_window(since: datetime, page_size: int, limit: int | None) -> Iterator[dict]:
    """Keyset-paginate by INCIDENT_NUMBER rather than OFFSET: if the source
    table gets reloaded mid-pull, OFFSET paging silently skips/duplicates
    rows; keyset paging at worst re-reads a few."""
    since_str = since.strftime("%Y-%m-%d %H:%M:%S")
    last_key, fetched = "", 0
    session = requests.Session()
    while True:
        # last_key comes from the source data; escape quotes before inlining.
        safe_key = last_key.replace("'", "''")
        sql = (
            f'SELECT * FROM "{CRIME_RESOURCE_ID}" '
            f"WHERE \"OCCURRED_ON_DATE\" >= '{since_str}' "
            f"AND \"INCIDENT_NUMBER\" > '{safe_key}' "
            f'ORDER BY "INCIDENT_NUMBER" LIMIT {page_size}'
        )
        rows = _get_with_retry(session, sql)
        if not rows:
            return
        for row in rows:
            yield row
            fetched += 1
            if limit and fetched >= limit:
                return
        last_key = rows[-1]["INCIDENT_NUMBER"]


def _get_with_retry(session: requests.Session, sql: str, attempts: int = 4) -> list[dict]:
    for i in range(attempts):
        try:
            r = session.get(CKAN_SQL_URL, params={"sql": sql}, timeout=60)
            r.raise_for_status()
            body = r.json()
            if not body.get("success"):
                raise RuntimeError(body.get("error"))
            return body["result"]["records"]
        except (requests.RequestException, RuntimeError, ValueError) as e:
            if i == attempts - 1:
                raise
            wait = 2 ** i
            log.warning("source request failed (%s); retrying in %ss", e, wait)
            time.sleep(wait)
    return []


# --- State --------------------------------------------------------------------
class SentState:
    """incident_number -> content_hash of the last version Kafka acked."""

    def __init__(self, path: str) -> None:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS sent ("
            " incident_number TEXT PRIMARY KEY,"
            " content_hash TEXT NOT NULL,"
            " sent_at TEXT NOT NULL)"
        )
        self.db.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)")

    def get_meta(self, k: str) -> str | None:
        row = self.db.execute("SELECT v FROM meta WHERE k = ?", (k,)).fetchone()
        return row[0] if row else None

    def set_meta(self, k: str, v: str) -> None:
        self.db.execute(
            "INSERT INTO meta VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v", (k, v)
        )
        self.db.commit()

    def get(self, incident: str) -> str | None:
        row = self.db.execute(
            "SELECT content_hash FROM sent WHERE incident_number = ?", (incident,)
        ).fetchone()
        return row[0] if row else None

    def mark_sent(self, pairs: list[tuple[str, str]]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.db.executemany(
            "INSERT INTO sent VALUES (?, ?, ?) ON CONFLICT(incident_number) "
            "DO UPDATE SET content_hash = excluded.content_hash, sent_at = excluded.sent_at",
            [(k, h, now) for k, h in pairs],
        )
        self.db.commit()


def source_last_modified() -> str:
    r = requests.get(CKAN_RESOURCE_SHOW_URL, params={"id": CRIME_RESOURCE_ID}, timeout=30)
    r.raise_for_status()
    return r.json()["result"]["last_modified"]


# --- Main loop --------------------------------------------------------------
def run_once(lookback_days: int, limit: int | None, dry_run: bool) -> dict:
    since = datetime.now(BOSTON_TZ).replace(tzinfo=None) - timedelta(days=lookback_days)
    state = SentState(STATE_DB_PATH)
    producer = None if dry_run else make_producer("crime-producer")
    tracker = DeliveryTracker()
    pending_hash: dict[str, str] = {}
    stats = {"read": 0, "unchanged": 0, "produced": 0, "dlq": 0, "no_location": 0}

    for raw in fetch_window(since, CRIME_PAGE_SIZE, limit):
        stats["read"] += 1
        try:
            event = normalize_crime(raw)
        except ValidationError as e:
            stats["dlq"] += 1
            if producer:
                producer.produce(
                    DLQ_TOPIC,
                    value=encode({"error": str(e), "raw": raw, "source": CRIME_RESOURCE_ID}),
                )
            continue

        key = event["incident_number"]
        if state.get(key) == event["content_hash"]:
            stats["unchanged"] += 1
            continue
        if not event["has_location"]:
            stats["no_location"] += 1
        stats["produced"] += 1
        pending_hash[key] = event["content_hash"]
        if producer:
            producer.produce(CRIME_TOPIC, key=key.encode(), value=encode(event), on_delivery=tracker)
            producer.poll(0)  # serve delivery callbacks without blocking

    if producer:
        remaining = producer.flush(60)
        if remaining:
            log.error("%d messages not delivered before timeout", remaining)
        # Only acked keys advance state -> at-least-once, never at-most-once.
        state.mark_sent([(k, pending_hash[k]) for k in tracker.acked_keys if k in pending_hash])
        stats["acked"], stats["failed"] = tracker.ok, tracker.failed

    log.info("run complete: %s", stats)
    return stats


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--lookback-days", type=int, default=CRIME_LOOKBACK_DAYS)
    p.add_argument("--limit", type=int, default=None, help="cap rows read (for demos)")
    p.add_argument("--dry-run", action="store_true", help="fetch + validate, don't produce")
    p.add_argument("--watch", type=int, default=0, metavar="SECONDS",
                   help="check source last_modified every N seconds; pull only on change")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    if not args.watch:
        stats = run_once(args.lookback_days, args.limit, args.dry_run)
        return 1 if stats.get("failed") else 0

    state = SentState(STATE_DB_PATH)
    while True:
        try:
            current = source_last_modified()
            seen = state.get_meta("source_last_modified")
            if current != seen:
                log.info("source published new version (%s, previously %s); pulling", current, seen)
                stats = run_once(args.lookback_days, args.limit, args.dry_run)
                # Only remember this version if the pull fully succeeded, so a
                # failed pull is retried on the next check.
                if not args.dry_run and not stats.get("failed"):
                    state.set_meta("source_last_modified", current)
            else:
                log.info("no new source version (last_modified=%s)", current)
        except requests.RequestException as e:
            log.error("source check/pull failed, retrying next interval: %s", e)
        time.sleep(args.watch)


if __name__ == "__main__":
    sys.exit(main())
