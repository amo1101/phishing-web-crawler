from __future__ import annotations
import time, requests
from typing import List, Dict

def cdx_latest_snapshots_for_url(url: str, n: int, cdx_endpoint: str, base_params: Dict[str, str], rps: float) -> List[str]:
    """
    Return latest N timestamps (YYYYmmddHHMMSS) for this specific URL (status 200, collapsed by digest).
    """
    params = dict(base_params)
    params["url"] = url
    resp = requests.get(cdx_endpoint, params=params, timeout=30)
    resp.raise_for_status()
    rows = resp.json()
    stamps = sorted({row[0] for row in rows if isinstance(row, list) and len(row) >= 4}, reverse=True)[:n]
    time.sleep(max(0, 1.0/rps))
    return stamps
