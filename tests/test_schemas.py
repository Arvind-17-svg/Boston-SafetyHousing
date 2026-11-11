import pytest

from ingestion.schemas import ValidationError, normalize_crime, validate_listing_state

RAW = {
    "_id": 1,
    "INCIDENT_NUMBER": "262095918",
    "OFFENSE_CODE": "3115",
    "OFFENSE_DESCRIPTION": "INVESTIGATE PERSON",
    "DISTRICT": "C6",
    "REPORTING_AREA": "214",
    "SHOOTING": "0",
    "OCCURRED_ON_DATE": "2026-11-01 18:10:00+00",
    "DAY_OF_WEEK": "Sunday   ",
    "HOUR": "18",
    "STREET": "W BROADWAY & F ST\nSOUTH BOSTON, MA 02127\nUNITED ST",
    "Lat": "42.3355",
    "Long": "-71.0500",
}


def test_timestamp_reinterpreted_as_boston_local():
    e = normalize_crime(RAW)
    # 2026-11-01 is after DST ends -> EST (-05:00)
    assert e["occurred_at_local"] == "2026-11-01T18:10:00-05:00"
    assert e["occurred_at_utc"] == "2026-11-01T23:10:00+00:00"


def test_street_cleaned_and_geohash_set():
    e = normalize_crime(RAW)
    assert e["street"] == "W BROADWAY & F ST"
    assert e["has_location"] and len(e["geohash"]) == 7


@pytest.mark.parametrize("lat,lon", [(None, None), ("0", "0"), ("-1", "-1"), ("40.7", "-74.0")])
def test_missing_or_bad_coords_kept_without_location(lat, lon):
    e = normalize_crime({**RAW, "Lat": lat, "Long": lon})
    assert not e["has_location"] and e["geohash"] is None and e["lat"] is None


def test_hash_changes_when_geocode_backfilled_but_not_on_reingest():
    before = normalize_crime({**RAW, "Lat": None, "Long": None})
    again = normalize_crime({**RAW, "Lat": None, "Long": None})
    after = normalize_crime(RAW)
    assert before["content_hash"] == again["content_hash"]  # ingested_at excluded
    assert before["content_hash"] != after["content_hash"]


@pytest.mark.parametrize("field", ["INCIDENT_NUMBER", "OCCURRED_ON_DATE"])
def test_required_fields(field):
    with pytest.raises(ValidationError):
        normalize_crime({**RAW, field: None})


def test_bad_timestamp():
    with pytest.raises(ValidationError):
        normalize_crime({**RAW, "OCCURRED_ON_DATE": "yesterday-ish"})


def test_listing_validation():
    ok = {"listing_id": "x", "lat": 42.35, "lon": -71.08, "price": 3000, "bedrooms": 2, "status": "active"}
    assert validate_listing_state(ok)["geohash"].startswith("drt")
    with pytest.raises(ValidationError):
        validate_listing_state({**ok, "status": "removed"})
    with pytest.raises(ValidationError):
        validate_listing_state({**ok, "price": 0})


# --- 311 -----------------------------------------------------------------------
from ingestion.schemas import normalize_service_request  # noqa: E402

RAW_311 = {
    "service_request_id": "69485739-eac5-4357-9ec4-98b641980a59",
    "status": "open",
    "service_name": "Needle Pickup",
    "service_code": "abc",
    "requested_datetime": "2026-11-11T16:54:28Z",
    "updated_datetime": "2026-11-11T17:23:38Z",
    "address": "101 Elm St, Boston, MA 02129",
    "lat": 42.379947,
    "long": -71.0617725,
    "token": "secret-token",
    "description": "guy named Bob at 555-1234 left needles",
    "status_notes": "call back resident",
    "media_url": "https://example.com/photo.jpg",
}


def test_311_labeled_and_excluded_from_safety_score():
    e = normalize_service_request(RAW_311)
    assert e["signal_class"] == "reported_condition"
    assert e["safety_score_eligible"] is False
    assert e["condition_category"] == "public_space"


def test_311_drops_free_text_and_tokens():
    e = normalize_service_request(RAW_311)
    for dropped in ("token", "description", "status_notes", "media_url"):
        assert dropped not in e
    assert "Bob" not in str(e)


def test_311_status_change_changes_hash():
    open_ = normalize_service_request(RAW_311)
    closed = normalize_service_request(
        {**RAW_311, "status": "closed", "updated_datetime": "2026-11-11T18:00:00Z"})
    assert open_["content_hash"] != closed["content_hash"]


def test_311_unknown_service_falls_back_to_other():
    assert normalize_service_request({**RAW_311, "service_name": "New Thing"})["condition_category"] == "other"


def test_311_utc_parsing_and_required_fields():
    e = normalize_service_request(RAW_311)
    assert e["updated_at_utc"] == "2026-11-11T17:23:38+00:00"
    with pytest.raises(ValidationError):
        normalize_service_request({**RAW_311, "service_request_id": None})
    with pytest.raises(ValidationError):
        normalize_service_request({**RAW_311, "updated_datetime": "2026-11-11 17:23:38"})
