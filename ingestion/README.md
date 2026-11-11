# Ingestion layer (Kafka)

Producers that pull Boston public data (and synthetic listings) into Kafka.
This is the input to the Spark Structured Streaming job (phase 5).

## Quick start

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d --wait               # Kafka (KRaft, single broker) + UI on :8080
python -m ingestion.create_topics         # idempotent

python -m ingestion.crime_producer        # one-shot pull (backfill / cron)
python -m ingestion.crime_producer --watch 600   # pull whenever the city publishes
python -m ingestion.open311_producer      # polls every 60s, runs forever
python -m ingestion.listing_producer      # synthetic listing events

python -m ingestion.inspect_consumer      # read everything back, print summary
pytest -q
```

## Topics

| Topic | Key | Cleanup | Retention | Semantics |
|---|---|---|---|---|
| `crime-events` | `incident_number` | delete | 90 d | Upsert stream: incidents are re-sent when the city backfills coordinates |
| `service-request-events` | `service_request_id` | delete | 30 d | Upsert stream: open → closed transitions. **Labeled conditions signal, not safety** |
| `listing-events` | `listing_id` | **compact** | ∞ (latest per key) | Table: full listing state per message, tombstone (null) = delisted |
| `ingestion-dlq` | — | delete | 30 d | Records that failed validation, with error + `source` |

## Source freshness

| Source | Freshness | How we ingest | Real-time? |
|---|---|---|---|
| Crime (BPD via CKAN) | Published daily; incidents ~2–3 days old | Watch `last_modified`, pull 30-day window on change, diff by content hash | **No**, and can't be |
| 311 (Open311) | ~1 minute | Poll every 60s with `updated_after` watermark + 120s overlap | Near-real-time (polling, no push) |
| Listings | Synthetic | Continuous event generator | Yes (simulated) |

## Tradeoffs

**Why Kafka at all?** Crime data alone doesn't justify it: ~200 events/day
and a daily batch. A cron job writing Parquet would do. Kafka is justified by:
1. *Fan-out:* multiple independent consumers (lake raw-zone writer, Spark
   feature aggregation, score updater → Redis/WebSocket, LLM blurb
   invalidation) each read at their own pace without the producer knowing them.
2. *Replay:* reset a consumer group's offsets to reprocess history with new
   feature/scoring logic through the same code path as live data.
3. *Genuinely live streams:* 311 and listings change by the minute.

If the downstream consumers don't get built, Kafka here is decoration.
Say so.

**Change detection via local SQLite state.** The CKAN crime table is
truncated and reloaded daily, so `_id` is not a stable cursor and rows are
updated in place. Producers hash each normalized record and send only
new/changed ones. *Cost:* producer state lives outside Kafka (losing
`.state/` means one full re-send, which consumers dedupe anyway).
*Alternative:* send everything every run and dedupe in Spark. Simpler, but
noisier.

**At-least-once, not exactly-once.** State (and the 311 watermark) advances
only after broker ack. A crash between ack and state write re-sends records.
Consumers must be idempotent on `(key, content_hash)`, which they need to be
anyway because upserts are legitimate. Kafka transactions would add
complexity without removing that requirement.

**Keyed by entity ID, not geohash.** Keys give per-entity ordering (an
incident's updates arrive in order) and even partition load. Keying by
geohash would co-locate cells, but Spark shuffles by cell anyway, and
dense downtown cells would make hot partitions.

**Geohash precision 7 (~150 m).** Coarser cells are string prefixes
(`gh[:6]` ≈ 1.2 × 0.6 km), so we store the finest grain and roll up downstream.

**JSON + `schema_version`, not Avro + Schema Registry.** Validation happens
in Python before produce; invalid records go to the DLQ. Avro would enforce
compatibility at the broker boundary but adds a service to run. Revisit when
Spark starts consuming.

**311 is labeled, not scored.** 311 volume reflects reporting behavior
(who calls), not only conditions. It's carried with
`signal_class="reported_condition"` and `safety_score_eligible=false`, and
the free-text `description` / `status_notes` / `media_url` fields are dropped
at ingestion because they can contain personal information.

**Single broker, RF=1.** Dev only. Production would use ≥3 brokers with
`min.insync.replicas=2`, otherwise `acks=all` guarantees nothing.

## Data-quality facts (verified against live APIs, Nov 2026)

- Crime `OCCURRED_ON_DATE` is labeled `+00` but is Boston local time: the
  quietest hours are 03:00–05:00 local. We emit `occurred_at_local` and
  `occurred_at_utc`.
- ~5% of all crime rows, and ~33–45% of the last 30 days, lack coordinates
  (geocoding is backfilled). They are emitted with `has_location=false` and
  re-emitted when coordinates appear.
- Crime `STREET` contains embedded newlines; `DAY_OF_WEEK` is space-padded.
- Open311 caps `per_page` at 300, orders by `updated_datetime` desc, and
  honors `updated_after`.
- ~23% of 311 requests fall in the `other` category; the mapping in
  `schemas.CONDITION_CATEGORIES` should grow as types are observed.
