# Boston Safety-Aware Housing Map — Project Context

This file is the persistent context for Claude Code and for anyone working
on this repo. Read it before making architectural decisions or scaffolding
new pieces — it captures the decisions already made and why, so they don't
get re-litigated or silently reversed.

## One-line pitch

A housing search tool for Boston that scores every listing against
historical crime data using a trained ML model, explains the score in
plain English via a retrieval-augmented LLM, and pushes live listing and
neighborhood-condition updates to the map.

## Project philosophy (read this first)

- **This is a data engineering + AI portfolio project, not a product.**
  The goal is to demonstrate depth in streaming pipelines, lakehouse
  design, ML, and LLM integration — not to ship a polished consumer app.
- **Frontend is intentionally thin.** React is a rendering layer only —
  map, listing cards, safety badges, a filter panel, a WebSocket hook.
  It owns zero business logic. Do not let frontend scope creep; if a
  decision could live in the backend/data layer instead of React, put
  it there.
- **Prefer full, working implementations over stubs or conceptual
  outlines.** Complete DDL, complete extractors, complete pipeline code,
  working code — not pseudocode or "TODO: implement this."
- **Be explicit about architectural tradeoffs, not just impressive-
  sounding tech.** See "Known tradeoffs" below — these are meant to be
  stated out loud in documentation/READMEs, not hidden.

## Current status (as of 2026-11-11)

| Piece | Status |
|---|---|
| Kafka (local, Docker, KRaft single broker) | ✅ Built — `docker-compose.yml` |
| Producers: crime, 311, synthetic listings | ✅ Built + tested end to end — `ingestion/` (see `ingestion/README.md`) |
| Debug consumer | ✅ `ingestion/inspect_consumer.py` (verification tool, not a real consumer) |
| Databricks Step 0 connectivity check | ⏳ **Next** — see "Databricks Free Edition: Step 0" |
| Local bronze writer (Kafka → UC Volume) | ⏳ Not started |
| Databricks bronze / silver / gold pipeline | ⏳ Not started |
| Features, model, scores, blurbs | ⏳ Not started |
| Hot-path live consumer, Redis, FastAPI, WebSocket | ⏳ Not started |
| React frontend | ⏳ Not started |

Note: the Kafka ingestion layer was built ahead of the original phase
order (it was phase 5). The build order below reflects what remains.

## Architecture

Decided 2026-11-11: **Databricks Free Edition** is the lakehouse / ML
platform. Free Edition's limits (serverless only, `AvailableNow` triggers
only, daily fair-usage quota, restricted outbound internet) shape the
design into two paths:

```
SOURCES (laptop)                   HOT PATH (laptop, ~1 min freshness)
────────────────                   ───────────────────────────────────
crime_producer   ─┐                Kafka ─▶ live consumer ─▶ Redis ─▶ FastAPI ─▶ WebSocket ─▶ React
open311_producer ─┼─▶ Kafka ──┐      • 311 conditions update within ~1 min
listing_producer ─┘           │      • new/changed listings scored immediately from
                              │        cell features + model synced from Databricks
                              │
                              │    COLD PATH (Databricks Free Edition, hourly/daily)
                              │    ─────────────────────────────────────────────────
                              └─▶ local bronze writer ─▶ UC Volume (JSON files)
                                    ─▶ Auto Loader, Trigger.AvailableNow (hourly job)
                                    ─▶ BRONZE  raw Kafka records, append-only
                                    ─▶ SILVER  parsed, deduped, latest version per key, H3 cells
                                    ─▶ GOLD    crime cell features ─▶ Feature Engineering in UC
                                               311 cell conditions (SEPARATE, never a feature)
                                               listing scores ◀─ LightGBM (MLflow, daily)
                                               cell blurbs (LLM, cached per cell)
                                    ─▶ sync job (hourly, 2X-Small SQL warehouse) ─▶ Redis
```

- **Databricks does history, curation, features, training, and batch
  scoring** on an hourly/daily cadence. That matches the data: crime is
  published daily.
- **The live experience stays local.** 311 and listing changes flow
  Kafka → live consumer → Redis → WebSocket without touching Databricks.
- **Kafka has three real consumers:** bronze writer, live consumer, blurb
  invalidator. This is what justifies Kafka (see tradeoff #2).

## Tech stack

| Layer | Choice | Notes |
|---|---|---|
| Ingestion | Kafka (`confluent-kafka` Python client), local Docker | Topics: `crime-events`, `listing-events`, `service-request-events`, `ingestion-dlq` — see `ingestion/README.md` |
| Kafka → Databricks | **Path B: local bronze writer → UC Volume → Auto Loader** | Laptop pushes files to Databricks (Files API + PAT), so Free Edition's outbound restrictions don't matter. Path A (Spark reads cloud Kafka directly) only if Step 0 shows it's reachable — optional |
| Lakehouse platform | **Databricks Free Edition** | Serverless only; see constraints below |
| Stream processing | **Spark Structured Streaming** (PySpark), `Trigger.AvailableNow()` | Scheduled micro-batch, not always-on. See tradeoff #1 |
| Table format | **Delta Lake**, Unity Catalog catalog `bsh` (schemas `landing`, `bronze`, `silver`, `gold`) | Bronze/silver/gold = raw zone / curated zone / serving-ready |
| Table layout | **Liquid clustering** on `(h3_cell, event_date)`; **no partitioning** | Replaces the old "partition by geohash + time" plan — data is far too small; partitioning would create thousands of tiny files |
| Upserts (silver) | `foreachBatch` + `MERGE` keyed by entity ID, update only when `content_hash` differs | Default. Switch to Lakeflow `AUTO CDC` only if Step 0 confirms it works on Free Edition (open decision) |
| Spatial index | **H3** (Databricks built-in `h3_longlatash3`), resolution 9 | Replaces geohash for modeling. `h3_kring` neighbor smoothing fixes the cell-edge problem. Producers still emit `geohash`; harmless, can be dropped |
| Feature store | Databricks Feature Engineering in Unity Catalog, timestamp key | Point-in-time lookups so training rows never see future data |
| ML model | LightGBM (or XGBoost), MLflow tracking + UC model registry | Time-based train/test split (never random). Compare vs naive baseline (trailing 30-day count). Retrain daily |
| LLM layer | RAG + Claude, blurbs cached per H3 cell | Where it runs is an open decision (Step 0): `ai_query()` in Databricks if available, else a local job calling the Claude API and writing back |
| Serving | FastAPI + Redis (local) | Never query Databricks per request. Hourly sync job exports gold → Redis |
| Frontend | React + Mapbox GL JS | Map, listing cards, safety badges, filter panel, WebSocket hook — no business logic |
| Code / deployment | Databricks Asset Bundles in this git repo; Databricks Connect for local dev | Workspace must be fully rebuildable from git (Free Edition accounts can be deleted) |
| Crime data source | Analyze Boston (**CKAN** datastore API, not Socrata) | Table is truncated + reloaded daily; incidents appear ~2–3 days after occurring |
| Conditions data source | Boston 311 via Open311 GeoReport v2 | Near-real-time (~1 min). **Labeled signal only — never a safety-score feature** (tradeoff #4) |
| Listings data source | Synthetic dataset | Zillow API discontinued; real listing APIs have ToS/access constraints |

## Databricks Free Edition constraints

Source: Databricks docs, "Free Edition limitations" and "Serverless
streaming", checked 2026-11-11. Re-check before relying on them.

| Limit | Consequence for this project |
|---|---|
| Serverless only; streaming only with `Trigger.AvailableNow()` (no trigger → `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`) | Scheduled jobs, not always-on streams. Every `writeStream` must set `.trigger(availableNow=True)` |
| Fair-usage quota, no published numbers; exceeding it shuts compute off for the day (extreme: month) | Cold path runs **hourly**, not every few minutes. Measure quota usage in week one; fall back to daily if needed |
| Outbound internet restricted to an unpublished allowlist | Assume Databricks can't reach Kafka, `data.boston.gov`, or `api.anthropic.com` until Step 0 proves otherwise |
| 5 concurrent job tasks; 1 active Lakeflow pipeline per type | One pipeline for bronze → silver → gold; keep jobs few |
| 1 SQL warehouse, 2X-Small | Used by the hourly gold → Redis sync |
| No online tables; limited Model Serving; some models unavailable | Serving stays local (Redis + FastAPI) |
| Inactive accounts may be deleted; no SLA | Databricks is never the only copy of anything unrebuildable. Crime/311 history can be re-fetched from city APIs; listings regenerate from the seed; code lives in git |
| Non-commercial use only | Fine — portfolio project |

## Databricks Free Edition: Step 0 (do this first)

A ~10-minute check in a Free Edition notebook that settles the open
decisions below. Record results in this file.

```python
import socket, requests
for url in ["https://data.boston.gov/api/3/action/status_show",
            "https://boston2-production.spotmobile.net/open311/v2/requests.json?per_page=1",
            "https://api.anthropic.com"]:
    try: print("OK     ", requests.get(url, timeout=10).status_code, url)
    except Exception as e: print("BLOCKED", type(e).__name__, url)

# Optional, only if trying Path A with a Confluent Cloud trial cluster:
host = "pkc-XXXX.us-east-1.aws.confluent.cloud"
try: socket.create_connection((host, 9092), timeout=10); print("OK      kafka", host)
except Exception as e: print("BLOCKED kafka", e)
```

Also check:
- [ ] Can you create a **personal access token**? (Required for Path B.)
- [ ] Does `ai_query()` / Foundation Model APIs work? (Decides where blurbs are generated.)
- [ ] Do Lakeflow pipelines with `AUTO CDC` run? (Decides MERGE vs declarative.)
- [ ] Can you create a feature table with a timestamp key in Feature Engineering?

**Step 0 results:** _not yet run_

## Open decisions

| # | Decision | Default until decided | Decided by |
|---|---|---|---|
| 1 | Silver upserts: hand-written `MERGE` vs Lakeflow `AUTO CDC` | Hand-written `MERGE` | Step 0 |
| 2 | Where LLM blurbs are generated | Local job calling Claude API, writing back to `gold.cell_blurbs` | Step 0 |
| 3 | Cold-path cadence: hourly vs daily | Hourly | Quota usage in week one |
| 4 | H3 resolution 9 vs 8 | 9 (~0.1 km²), roll up with `h3_toparent` | Feature/model evaluation |
| 5 | Add Path A (Spark reads cloud Kafka directly) | No — Path B only | Step 0 + whether it's worth a Confluent trial |

## Known tradeoffs (state these explicitly in docs)

1. **Spark Structured Streaming over Flink.** Latency was never the
   deciding factor — neither engine's latency matters here (crime is
   published daily). Flink is actually the better fit for this data's
   *shape*: all three streams are upserts (crime geocode backfills,
   311 open→closed, listing changes/deletes), and Flink SQL handles
   changelogs natively with retractions, whereas Spark needs the
   `foreachBatch` + `MERGE`-into-Delta pattern and computes aggregates
   over tables. We chose Spark anyway for the Databricks/Delta ecosystem,
   the integrated ML path (Feature Engineering, MLflow), and PySpark's
   maturity vs PyFlink. Flink would have meant Iceberg instead of Delta
   and a non-Databricks stack.
2. **Kafka/streaming is partly architectural demonstration.** Crime
   data isn't real-time — it's a daily batch feed of ~200 events/day.
   Kafka is justified by **fan-out** (bronze writer, live consumer, blurb
   invalidator read independently), **replay** (90-day retention on
   `crime-events`), and the **genuinely live streams** (311, listings) —
   not by volume or crime latency. If those consumers aren't built, a
   cron job writing Parquet would be the honest choice. Say so in any
   write-up.
3. **Listings data is synthetic.** Real listing sources (Zillow,
   Redfin, Craigslist) have discontinued APIs or ToS restrictions on
   scraping. Synthetic listings placed on real Boston street names keep
   focus on the data/ML/AI work, which is the point of the project.
4. **311 is a labeled conditions signal, never a safety input.**
   (Decided 2026-11-11.) Boston 311 is the only near-real-time public
   source, but its volume reflects who files reports, not only what
   happens — areas with more active reporters look "worse". Mixing it
   into the safety score would bake reporting bias into a
   neighborhood-ranking product. It lives in its own gold table
   (`gold.cell_conditions_311`), is never joined into safety features,
   and every event carries `signal_class="reported_condition"` and
   `safety_score_eligible=false`. Free-text fields (description,
   status_notes, media_url) are dropped at ingestion for privacy.
5. **Crime data cannot be made real-time.** The source publishes daily,
   and incidents land ~2–3 days after occurring. The crime producer
   watches the dataset's `last_modified` and pulls on publish, which
   removes our latency but not the source's.
6. **Databricks Free Edition over Snowflake, self-hosted Spark, or a
   paid tier.** (Decided 2026-11-11.) Free Edition doesn't expire (a
   portfolio demo must stay up), and keeps Spark Structured Streaming +
   Delta. Snowflake was rejected because Snowpark Connect does not
   support Structured Streaming and its trial expires after 30 days.
   Cost of the choice: no always-on streaming, an unpublished compute
   quota, restricted outbound network, and no SLA — hence the hot/cold
   path split.
7. **Freshness is split, not "real-time" end to end.** ~1 minute for 311
   conditions and listing changes (hot path); ~1 hour for features and
   scores (cold path); ~2–3 days for crime itself (source). Don't
   describe the whole system as real-time.
8. **H3 over geohash for modeling.** Geohash cells have an edge problem
   (a listing near a border ignores crimes just across it) and uneven
   neighbor distances. H3 hexagons are uniform, `h3_kring` gives
   neighbor smoothing, and Databricks has H3 built in.

## Work breakdown

### Data engineering (primary track)
- ✅ Kafka ingestion of crime, 311, and listing events
- Local bronze writer: Kafka → batched JSON → UC Volume (Files API)
- Bronze: Auto Loader (`cloudFiles`, `AvailableNow`) → append-only raw tables
- Silver: parse with explicit schemas (bad rows → `silver.quarantine`),
  latest version per key via `MERGE`, listing tombstones → `DELETE`,
  H3 cells via `h3_longlatash3`
- Gold: crime features per H3 cell + date (7/30/90-day counts, category
  mix, trend slope, time-of-day mix, `h3_kring`-smoothed counts); 311
  conditions in a separate table
- Asset Bundles for all jobs/pipelines

### AI/ML (primary track)
- Feature Engineering in UC with point-in-time lookups
- Safety scoring model: LightGBM predicting risk score + trend
  direction, time-based split, vs naive baseline, MLflow + UC registry
- Batch scoring job → `gold.listing_scores`
- LLM narrative layer: RAG generating cached per-H3-cell blurbs
  (never call the LLM per request); regenerate only when a cell's
  features change meaningfully
- Hot path: live consumer scores new/changed listings from synced cell
  features + model; updates Redis; publishes over WebSocket
- Serving API: FastAPI reading from Redis, REST + WebSocket

### Frontend (thin — minimal effort by design)
- Map view (Mapbox GL JS), listing cards, safety badges, filter panel
- Separate, clearly labeled 311 "reported conditions" layer — visually
  distinct from safety badges
- WebSocket hook subscribing to live updates
- Calls one API endpoint — no local business logic

## Build order (remaining)

1. **Step 0** — Free Edition connectivity/feature check; record results
2. **Bronze** — local bronze writer + Auto Loader job
3. **Silver** — `MERGE` upserts, H3, quarantine
4. **Gold features** — crime cell features + separate 311 conditions
5. **ML** — Feature Engineering, LightGBM vs baseline, MLflow
6. **Scores** — batch scoring job
7. **Blurbs** — LLM layer, cached per cell
8. **Serving** — sync job → Redis → FastAPI + WebSocket; hot-path live consumer
9. **Frontend** — thin React map
10. **Polish** — quota tuning, auth, README write-up of all tradeoffs

Keep a working end-to-end demo at each step where possible.

## Data sources

- **Crime data:** Analyze Boston (data.boston.gov), CKAN datastore API,
  resource `b973d8cb-eeb2-4e7e-99da-c92938efc9c0` (2023–present).
  Boston Police Department incident reports — lat/long, offense, date,
  district. Quirks: `OCCURRED_ON_DATE` is labeled `+00` but is actually
  Boston local time (verified via hour-of-day distribution); ~33–45% of
  the last 30 days lack coordinates (backfilled later); table is
  truncated and reloaded daily so `_id` is not a stable cursor.
- **311 conditions:** Boston Open311 API
  (`boston2-production.spotmobile.net/open311/v2/requests.json`) —
  category, status, lat/long; updated within ~1 minute; `per_page` max
  300; supports `updated_after`.
- **Listings data:** synthetic (`ingestion/listing_producer.py`), real
  Boston neighborhoods and street names, synthetic prices/units.

## Conventions / preferences for this repo

- Full DDL, full extractor code, full pipeline and model training code —
  avoid partial scaffolds or "fill this in" placeholders
- Document tradeoffs inline (code comments or README) rather than
  silently picking the "impressive" option over the honest one
- Keep React scoped to rendering only — resist adding backend logic
  to the frontend for convenience
- Python deps in `.venv/` (`pip install -r requirements.txt`); Kafka in
  Docker (`docker compose up -d --wait`). See `ingestion/README.md`
- Every Databricks `writeStream` sets `.trigger(availableNow=True)`
- 311 data never enters safety features — enforce it in code review
