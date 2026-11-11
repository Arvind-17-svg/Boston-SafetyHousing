from ingestion.geohash import encode


def test_reference_vector():
    # Canonical example from the original geohash.org documentation.
    assert encode(57.64911, 10.40744, 11) == "u4pruydqqvj"


def test_boston_prefix_and_rollup():
    gh7 = encode(42.3601, -71.0589, 7)
    assert gh7.startswith("drt2")
    assert encode(42.3601, -71.0589, 6) == gh7[:6]
