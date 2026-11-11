"""Central configuration. Everything overridable via environment variables."""
import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# --- Topics -----------------------------------------------------------------
CRIME_TOPIC = os.getenv("CRIME_TOPIC", "crime-events")
LISTING_TOPIC = os.getenv("LISTING_TOPIC", "listing-events")
# One shared dead-letter topic for all sources; each message carries its
# `source`. Per-source DLQs would be cleaner at scale but are noise at this size.
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "ingestion-dlq")

# --- Analyze Boston (CKAN datastore, *not* Socrata) -------------------------
CKAN_SQL_URL = "https://data.boston.gov/api/3/action/datastore_search_sql"
# "Crime Incident Reports - 2023 to Present"
CRIME_RESOURCE_ID = os.getenv("CRIME_RESOURCE_ID", "b973d8cb-eeb2-4e7e-99da-c92938efc9c0")
# The source table is truncated and reloaded daily (_id is reassigned 1..N),
# so there is no stable append cursor. We re-pull a trailing window and
# diff against local state instead. 30 days catches late geocoding backfills.
CRIME_LOOKBACK_DAYS = int(os.getenv("CRIME_LOOKBACK_DAYS", "30"))
CRIME_PAGE_SIZE = int(os.getenv("CRIME_PAGE_SIZE", "5000"))

# Local state used for change detection (incident_number -> content hash).
STATE_DB_PATH = os.getenv("STATE_DB_PATH", ".state/crime_producer.sqlite")

# Precision 7 ~= 153m x 153m. Downstream can roll up to 6 (~1.2km x 0.6km)
# or 5 by string prefix, so we store the finest grain we'd ever want.
GEOHASH_PRECISION = 7

# Rough bounding box for Boston; coordinates outside it are treated as bad
# geocodes (the source uses 0/0 and sometimes -1/-1 as sentinels).
BOSTON_BBOX = {"lat_min": 42.20, "lat_max": 42.42, "lon_min": -71.20, "lon_max": -70.98}

# --- Boston 311 (Open311 GeoReport v2) ---------------------------------------
# A *labeled conditions signal*, NOT a crime or safety signal. It must never be
# used as a feature in the safety score (see README: reporting bias).
SERVICE_REQUEST_TOPIC = os.getenv("SERVICE_REQUEST_TOPIC", "service-request-events")
OPEN311_URL = os.getenv(
    "OPEN311_URL", "https://boston2-production.spotmobile.net/open311/v2/requests.json"
)
OPEN311_PAGE_SIZE = 300          # server caps per_page at 300
OPEN311_POLL_SECONDS = int(os.getenv("OPEN311_POLL_SECONDS", "60"))
# Re-read this much before the watermark each poll: guards against records
# sharing a timestamp second and against rows shifting between pages while
# we paginate (results are ordered by updated_datetime desc and move as they
# get updated). Duplicates this causes are filtered by content hash.
OPEN311_OVERLAP_SECONDS = 120
OPEN311_INITIAL_LOOKBACK_HOURS = int(os.getenv("OPEN311_INITIAL_LOOKBACK_HOURS", "6"))
OPEN311_STATE_DB_PATH = os.getenv("OPEN311_STATE_DB_PATH", ".state/open311_producer.sqlite")

# CKAN resource metadata, used to trigger crime pulls on publish.
CKAN_RESOURCE_SHOW_URL = "https://data.boston.gov/api/3/action/resource_show"
