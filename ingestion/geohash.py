"""Minimal geohash encoder (no third-party dependency).

Geohash is a Z-order curve over lat/lon. Its useful property here: a
precision-6 cell is literally the first 6 chars of the precision-7 cell, so
Spark can roll up spatially with substring() rather than a spatial join.
"""
_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def encode(lat: float, lon: float, precision: int = 7) -> str:
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        raise ValueError(f"coordinates out of range: {lat}, {lon}")
    lat_lo, lat_hi = -90.0, 90.0
    lon_lo, lon_hi = -180.0, 180.0
    out, bits, n_bits, use_lon = [], 0, 0, True
    while len(out) < precision:
        if use_lon:
            mid = (lon_lo + lon_hi) / 2
            if lon >= mid:
                bits, lon_lo = (bits << 1) | 1, mid
            else:
                bits, lon_hi = bits << 1, mid
        else:
            mid = (lat_lo + lat_hi) / 2
            if lat >= mid:
                bits, lat_lo = (bits << 1) | 1, mid
            else:
                bits, lat_hi = bits << 1, mid
        use_lon = not use_lon
        n_bits += 1
        if n_bits == 5:
            out.append(_BASE32[bits])
            bits, n_bits = 0, 0
    return "".join(out)
