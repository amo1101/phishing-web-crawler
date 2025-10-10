from __future__ import annotations
import time, requests, logging
from typing import List, Dict

log = logging.getLogger(__name__)

def cdx_latest_snapshots_for_url(url: str, n: int, cdx_endpoint: str, base_params: Dict[str, str], rps: float) -> List[str]:
    params = dict(base_params)
    params["url"] = url
    log.info("CDX lookup for url=%s n=%d endpoint=%s", url, n, cdx_endpoint)
    try:
        resp = requests.get(cdx_endpoint, params=params, timeout=30)
        log.debug("CDX response: status=%s", resp.status_code)
        resp.raise_for_status()
        rows = resp.json()
        log.debug(f'CDX response: {resp.json()}')
    except Exception:
        log.exception("CDX query failed for %s", url)
        raise
    stamps = sorted({row[0] for row in rows[1:] if isinstance(row, list) and len(row) >= 4}, reverse=True)[:n]
    log.info("CDX latest timestamps for %s: %s", url, stamps)
    time.sleep(max(0, 1.0 / rps))
    return stamps
