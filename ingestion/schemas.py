"""Record normalization + validation for events written to Kafka.

Serialization choice: JSON with an explicit `schema_version` field, validated
in Python before produce. See README "Tradeoffs" for why this isn't Avro +
Schema Registry yet.

Data-quality facts about the Analyze Boston crime feed that this module
encodes (verified against the live API, Nov 2026):

* OCCURRED_ON_DATE is labeled "+00" but is actually Boston local wall-clock
  time. Evidence: the HOUR column equals the timestamp's hour, and the
  quietest hours are 03:00-05:00, which only makes sense as local time.
  We re-interpret it as America/New_York and emit both local and UTC.
* ~5% of all rows, and ~33% of rows from the last month, have no lat/long.
  Geocoding is backfilled later. We still emit these (has_location=false)
  so the raw zone is complete; a later re-pull emits an updated version once
  coordinates appear (detected via content_hash change).
* STREET can contain embedded newlines with city/zip/country fragments;
  DAY_OF_WEEK is space-padded.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from . import geohash
from .config import BOSTON_BBOX, GEOHASH_PRECISION

SCHEMA_VERSION = 1
BOSTON_TZ = ZoneInfo("America/New_York")


class ValidationError(ValueError):
    """Raised when a source record can't be turned into a valid event."""


def _clean_str(v) -> str | None:
    if v is None:
        return None
    s = " ".join(str(v).split())
    return s or None


def _parse_coord(v) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f


def _in_boston(lat: float | None, lon: float | None) -> bool:
    return (
        lat is not None
        and lon is not None
        and BOSTON_BBOX["lat_min"] <= lat <= BOSTON_BBOX["lat_max"]
        and BOSTON_BBOX["lon_min"] <= lon <= BOSTON_BBOX["lon_max"]
    )


def _parse_occurred_on(raw: str) -> datetime:
    """Parse '2026-11-01 18:10:00+00' as Boston *local* time (see module doc)."""
    try:
        naive = datetime.fromisoformat(raw.strip()).replace(tzinfo=None)
    except (AttributeError, ValueError) as e:
        raise ValidationError(f"unparseable OCCURRED_ON_DATE: {raw!r}") from e
    # fold=0: for the ambiguous 01:00-02:00 hour on DST fall-back day, pick
    # the first occurrence. The source carries no information to disambiguate.
    return naive.replace(tzinfo=BOSTON_TZ)


def content_hash(event: dict) -> str:
    """Stable hash over business fields only (excludes ingestion metadata)."""
    business = {k: v for k, v in event.items() if k not in ("ingested_at", "content_hash")}
    return hashlib.sha256(json.dumps(business, sort_keys=True).encode()).hexdigest()[:16]


def normalize_crime(raw: dict) -> dict:
    incident = _clean_str(raw.get("INCIDENT_NUMBER"))
    if not incident:
        raise ValidationError("missing INCIDENT_NUMBER")
    occurred_raw = raw.get("OCCURRED_ON_DATE")
    if not occurred_raw:
        raise ValidationError("missing OCCURRED_ON_DATE")
    occurred_local = _parse_occurred_on(occurred_raw)

    lat, lon = _parse_coord(raw.get("Lat")), _parse_coord(raw.get("Long"))
    has_location = _in_boston(lat, lon)
    if not has_location:
        lat = lon = None

    street = raw.get("STREET")
    street = _clean_str(str(street).split("\n")[0]) if street else None

    event = {
        "schema_version": SCHEMA_VERSION,
        "event_type": "crime_incident_upserted",
        "incident_number": incident,
        "offense_code": _clean_str(raw.get("OFFENSE_CODE")),
        "offense_description": _clean_str(raw.get("OFFENSE_DESCRIPTION")),
        "district": _clean_str(raw.get("DISTRICT")),
        "reporting_area": _clean_str(raw.get("REPORTING_AREA")),
        "shooting": _clean_str(raw.get("SHOOTING")) in ("1", "Y", "true", "True"),
        "occurred_at_local": occurred_local.isoformat(),
        "occurred_at_utc": occurred_local.astimezone(timezone.utc).isoformat(),
        "occurred_on_raw": occurred_raw,
        "street": street,
        "lat": lat,
        "lon": lon,
        "has_location": has_location,
        "geohash": geohash.encode(lat, lon, GEOHASH_PRECISION) if has_location else None,
    }
    event["content_hash"] = content_hash(event)
    event["ingested_at"] = datetime.now(timezone.utc).isoformat()
    return event


# --- Listings ---------------------------------------------------------------
LISTING_REQUIRED = ("listing_id", "lat", "lon", "price", "bedrooms", "status")
LISTING_STATUSES = {"active", "pending"}


def validate_listing_state(state: dict) -> dict:
    """Listing events carry the FULL current state (not a delta), because the
    topic is log-compacted: after compaction only the latest message per key
    survives, so each message must be self-sufficient."""
    missing = [k for k in LISTING_REQUIRED if state.get(k) is None]
    if missing:
        raise ValidationError(f"listing missing fields: {missing}")
    if state["status"] not in LISTING_STATUSES:
        raise ValidationError(f"bad status {state['status']!r}; delistings are tombstones")
    if not _in_boston(state["lat"], state["lon"]):
        raise ValidationError("listing outside Boston bbox")
    if state["price"] <= 0:
        raise ValidationError("non-positive price")
    out = dict(state)
    out["schema_version"] = SCHEMA_VERSION
    out["geohash"] = geohash.encode(state["lat"], state["lon"], GEOHASH_PRECISION)
    return out


# --- 311 service requests ---------------------------------------------------
# Fields we deliberately DROP (data minimization):
#   description, status_notes -- free text from residents/staff; can contain
#                                names, phone numbers, plate numbers
#   media_url                 -- photos, may show people/plates
#   token                     -- request-tracking token, no analytic value
# `address` is kept: it's published by the city alongside lat/long and is
# needed to display the request; it is not a person's identity.

# Coarse buckets for display/filtering. Unknown types fall into "other" and
# keep their raw service_name, so the mapping can grow without data loss.
CONDITION_CATEGORIES = {
    "sanitation": {
        "Litter & Debris", "Overflowing Trash", "Trash Placed Out Early",
        "Improper Trash Storage", "Illegal Dumping or Disposal", "Missed Trash",
        "Dead Animal General",
    },
    "street_infrastructure": {
        "Street Light Outage", "Pothole", "Sidewalk Repair", "Traffic Signal Repair",
        "Sign Repair",
    },
    "parking_traffic": {"Illegal Parking", "Abandoned Vehicle"},
    "public_space": {"Needle Pickup", "Illegal Graffiti"},
    "housing_building": {"Building Maintenance Violations", "Heat - Excessive Insufficient"},
    "animals": {"Domestic Animal Issue", "Rodent Activity"},
}
_CATEGORY_BY_SERVICE = {s: c for c, names in CONDITION_CATEGORIES.items() for s in names}


def _parse_utc(raw, field: str) -> datetime:
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError as e:
        raise ValidationError(f"unparseable {field}: {raw!r}") from e
    if dt.tzinfo is None:
        raise ValidationError(f"{field} has no timezone: {raw!r}")
    return dt.astimezone(timezone.utc)


def normalize_service_request(raw: dict) -> dict:
    srid = _clean_str(raw.get("service_request_id"))
    if not srid:
        raise ValidationError("missing service_request_id")
    if not raw.get("updated_datetime") or not raw.get("requested_datetime"):
        raise ValidationError("missing requested/updated datetime")
    service_name = _clean_str(raw.get("service_name"))

    lat, lon = _parse_coord(raw.get("lat")), _parse_coord(raw.get("long"))
    has_location = _in_boston(lat, lon)
    if not has_location:
        lat = lon = None

    event = {
        "schema_version": SCHEMA_VERSION,
        "event_type": "service_request_upserted",
        # Explicit label carried on every record so no downstream consumer can
        # mistake this for crime data.
        "signal_class": "reported_condition",
        "safety_score_eligible": False,
        "service_request_id": srid,
        "service_name": service_name,
        "service_code": _clean_str(raw.get("service_code")),
        "condition_category": _CATEGORY_BY_SERVICE.get(service_name, "other"),
        "status": _clean_str(raw.get("status")),
        "requested_at_utc": _parse_utc(raw["requested_datetime"], "requested_datetime").isoformat(),
        "updated_at_utc": _parse_utc(raw["updated_datetime"], "updated_datetime").isoformat(),
        "address": _clean_str(raw.get("address")),
        "lat": lat,
        "lon": lon,
        "has_location": has_location,
        "geohash": geohash.encode(lat, lon, GEOHASH_PRECISION) if has_location else None,
    }
    event["content_hash"] = content_hash(event)
    event["ingested_at"] = datetime.now(timezone.utc).isoformat()
    return event
