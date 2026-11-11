# Boston Safety-Aware Housing Map — Project Context

This file is the persistent context for Claude Code when working on this
repo. Read it before making architectural decisions or scaffolding new
pieces — it captures the decisions already made and why, so they don't
get re-litigated or silently reversed.

## One-line pitch

A real-time housing search tool for Boston that scores every listing
against historical crime data using a trained ML model, and explains
the score in plain English via a retrieval-augmented LLM.

## Project philosophy (read this first)

- **This is a data engineering + AI portfolio project, not a product.**
  The goal is to demonstrate depth in streaming pipelines, data lake
  design, ML, and LLM integration — not to ship a polished consumer app.
- **Frontend is intentionally thin.** React is a rendering layer only —
  map, listing cards, safety badges, a filter panel, a WebSocket hook.
  It owns zero business logic. Do not let frontend scope creep; if a
  decision could live in the backend/data layer instead of React, put
  it there.
- **Prefer full, working implementations over stubs or conceptual
  outlines.** Complete DDL, complete extractors, complete dbt models,
  working code — not pseudocode or "TODO: implement this."
- **Be explicit about architectural tradeoffs, not just impressive-
  sounding tech.** See "Known tradeoffs" below — these are meant to be
  stated out loud in documentation/READMEs, not hidden.

## Architecture

```
┌─────────────────────┐     ┌──────────────────────┐
│ Analyze Boston API   │     │ Listings source       │
│ (crime incidents)    │     │ (synthetic for now)   │
└──────────┬───────────┘     └───────────┬───────────┘
           │                             │
           └──────────────┬──────────────┘
                           ▼
                ┌─────────────────────┐
                │ Kafka ingestion      │
                │ (crime-events,       │
                │  listing-events)     │
                └──────────┬───────────┘
                           ▼
                ┌─────────────────────────────┐
                │ Spark Structured Streaming   │
                │ windowed spatial aggregation │
                │ (per geohash cell)           │
                └──────────┬───────────────────┘
                           ▼
                ┌─────────────────────┐
                │ Data lake             │
                │ raw zone + curated zone │
                │ partitioned by geohash/time │
                └──────────┬───────────┘
                           ▼
                ┌─────────────────────┐
                │ Feature store         │
                │ spatial-temporal      │
                │ features              │
                └──────────┬───────────┘
              ┌────────────┴────────────┐
              ▼                         ▼
   ┌─────────────────────┐   ┌─────────────────────────┐
   │ Safety scoring model  │   │ LLM narrative generator  │
   │ (XGBoost/LightGBM)    │   │ (RAG over crime summaries)│
   └──────────┬───────────┘   └────────────┬─────────────┘
              └────────────┬───────────────┘
                           ▼
                ┌─────────────────────┐
                │ Real-time serving     │
                │ FastAPI + Redis cache │
                │ REST + WebSocket      │
                └──────────┬───────────┘
                           ▼
                ┌─────────────────────┐
                │ React frontend (thin) │
                │ map, cards, badges    │
                └─────────────────────┘
```

## Tech stack

| Layer | Choice | Notes |
|---|---|---|
| Ingestion | Kafka (`confluent-kafka` Python client) | Topics: `crime-events`, `listing-events`, `service-request-events`, `ingestion-dlq` — see `ingestion/README.md` |
| Stream processing | **Spark Structured Streaming** (PySpark) | Chosen over Flink — see tradeoffs below |
| Data lake | Raw + curated zones, partitioned by geohash + time | Format TBD (Parquet/Delta — Delta preferred if pairing with future Databricks work) |
| Feature store | Spatial-temporal features | Crime density, trend slope, category mix, time-of-day distribution |
| ML model | XGBoost or LightGBM | Time-based train/test split (never random — crime is temporal) |
| LLM layer | RAG pipeline + Claude API | Pulls curated crime records per listing area, generates cached neighborhood blurbs |
| Serving | FastAPI + Redis | Thin layer: reads precomputed scores/blurbs from cache, exposes REST + WebSocket |
| Frontend | React + Mapbox GL JS | Map, listing cards, safety badges, filter panel, WebSocket hook — no business logic |
| Crime data source | Analyze Boston (**CKAN** datastore API, not Socrata) | Table is truncated + reloaded daily; incidents appear ~2–3 days after occurring |
| Conditions data source | Boston 311 via Open311 GeoReport v2 | Near-real-time (~1 min). **Labeled signal only — never a safety-score feature** (tradeoff #4) |
| Listings data source | Synthetic dataset (for now) | Zillow API discontinued; real listing APIs have ToS/access constraints |

## Known tradeoffs (state these explicitly in docs)

1. **Spark Structured Streaming over Flink.** Flink offers true event-
   at-a-time processing; Spark's default mode is micro-batch. This
   project doesn't need sub-second latency (crime data updates ~daily,
   listings aren't high-frequency either), so Flink's main advantage
   doesn't pay off here. Spark also keeps the stack consistent with a
   separately planned Databricks project, and PySpark's Python API is
   more mature than PyFlink's.
2. **Kafka/streaming is partly architectural demonstration.** Crime
   data isn't truly real-time — it's closer to a scheduled batch feed.
   The streaming layer is included to demonstrate the skill, not
   because the data demands it. A cron + batch job could deliver most
   of the actual business value with far less operational complexity.
   Be honest about this in any write-up.
3. **Listings data is synthetic.** Real listing sources (Zillow,
   Redfin, Craigslist) have discontinued APIs or ToS restrictions on
   scraping. A synthetic dataset seeded with real Boston addresses
   sidesteps this and keeps focus on the data/ML/AI work, which is the
   point of the project.
4. **311 is a labeled conditions signal, never a safety input.** (Decided
   2026-11-11.) Boston 311 is the only near-real-time public source, but
   its volume reflects who files reports, not only what happens — areas
   with more active reporters look "worse". Mixing it into the safety
   score would bake reporting bias into a neighborhood-ranking product.
   It is shown separately and clearly labeled; every event carries
   `signal_class="reported_condition"` and `safety_score_eligible=false`.
   Free-text fields (description, status_notes, media_url) are dropped at
   ingestion for privacy.
5. **Crime data cannot be made real-time.** The source publishes daily,
   and incidents land ~2–3 days after occurring. The crime producer
   watches the dataset's `last_modified` and pulls on publish, which
   removes our latency but not the source's. What justifies Kafka is
   fan-out to multiple consumers, replay, and the genuinely live streams
   (311, listings) — not crime volume (~200 events/day) or crime latency.

## Work breakdown

### Data engineering (primary track)
- Kafka ingestion of crime + listing events
- Spark Structured Streaming windowed spatial aggregation (rolling
  crime counts per geohash cell, by category and recency)
- Data lake: raw zone (unprocessed) + curated zone (cleaned, geocoded,
  deduplicated), partitioned by geohash and time
- Feature store: spatial-temporal features for the model and ad-hoc
  querying

### AI/ML (primary track)
- Safety scoring model: gradient-boosted model predicting risk score +
  trend direction, evaluated against a naive baseline, time-based
  train/test split
- LLM narrative layer: RAG pipeline generating cached per-area
  neighborhood blurbs (don't call the LLM per-request — cache by
  geohash/neighborhood)
- Serving API: FastAPI reading precomputed scores/blurbs from Redis,
  exposed over REST + WebSocket

### Frontend (thin — minimal effort by design)
- Map view (Mapbox GL JS), listing cards, safety badges, filter panel
- WebSocket hook subscribing to live score updates
- Calls one API endpoint — no local business logic

## Build order (six phases)

1. **Static MVP** — synthetic listings + one-time crime data pull,
   basic non-ML scoring formula, map and API working end to end
2. **Feature engineering** — data lake zones, feature store,
   spatial-temporal feature set
3. **ML scoring model** — trained gradient-boosted model replacing the
   static formula, evaluated vs. baseline
4. **LLM narrative layer** — RAG pipeline generating cached blurbs;
   optional AI chat Q&A on top
5. **Real-time infrastructure** — Kafka ingestion, Spark Structured
   Streaming, WebSocket broadcast
6. **Polish** — caching strategy, auth, deployment

Build in this order so there's always a working end-to-end demo, rather
than a half-finished pipeline with nothing to show.

## Data sources

- **Crime data:** Analyze Boston (data.boston.gov), CKAN datastore API,
  Boston Police Department crime incident reports — lat/long, offense
  type, date/time, district. `OCCURRED_ON_DATE` is labeled `+00` but is
  actually Boston local time (verified via hour-of-day distribution).
- **311 conditions:** Boston Open311 API — service requests with
  category, status, lat/long; updated within ~1 minute
- **Listings data:** synthetic, seeded with real Boston addresses

## Conventions / preferences for this repo

- Full DDL, full extractor code, full model training scripts — avoid
  partial scaffolds or "fill this in" placeholders
- Document tradeoffs inline (code comments or README) rather than
  silently picking the "impressive" option over the honest one
- Keep React scoped to rendering only — resist adding backend logic
  to the frontend for convenience