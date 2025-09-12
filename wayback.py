from __future__ import annotations
import time
from typing import List, Dict
import requests

def cdx_latest_snapshots_for_domain(domain: str, n: int, cdx_endpoint: str, base_params: Dict[str, str], rps: float) -> List[str]:
    """
    Returns list of latest timestamps (YYYYmmddHHMMSS) for the domain, unique by digest.
    """
    params = dict(base_params)
    params["url"] = domain
    # polite rate-limiting
    resp = requests.get(cdx_endpoint, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    # First row may be field names if output=json? Wayback CDX JSON returns rows (sometimes with header).
    # Defensive parse: rows with len>=4 -> (timestamp, original, status, digest)
    records = []
    for row in data:
        if not isinstance(row, list) or len(row) < 4:
            continue
        timestamp, original, statuscode, digest = row[0], row[1], row[2], row[3]
        records.append((timestamp, original))
    # pick latest timestamps
    stamps = sorted({ts for ts, _ in records}, reverse=True)[:n]
    time.sleep(max(0, 1.0 / rps))  # crude polite sleep after request
    return stamps
