from __future__ import annotations
from pathlib import Path
from datetime import date
from typing import Optional
import logging, requests
from typing import Dict, List, Tuple, Set
from pathlib import Path
import csv
from .normalize import normalize_url, registrable_domain

log = logging.getLogger(__name__)

def fetch_iosco_csv(
    csv_root: Path,
    start_date: Optional[date],
    end_date: Optional[date],
    *,
    nca_id: int = 64,
    subsection: str = "main",
    timeout: int = 60
) -> Path:
    csv_root.mkdir(parents=True, exist_ok=True)
    if start_date and end_date:
        csv_name = f"iosco_export_{start_date.isoformat()}_to_{end_date.isoformat()}.csv"
    else:
        from datetime import date as d
        csv_name = f"iosco_export_all_{d.today().isoformat()}.csv"
    out_path = csv_root / csv_name

    base_url = "https://www.iosco.org/i-scan/?export-to-csv"
    params = {"SUBSECTION": subsection, "NCA_ID": str(nca_id)}
    if start_date and end_date:
        params["ValidationDateStart"] = start_date.isoformat()
        params["ValidationDateEnd"]   = end_date.isoformat()

    log.info("Fetching IOSCO CSV: %s params=%s timeout=%ss -> %s", base_url, params, timeout, out_path)
    try:
        with requests.get(base_url, params=params, timeout=timeout, stream=True, headers={
            "User-Agent": "Mozilla/5.0 (compatible; FMA-Crawler/1.0)",
            "Accept": "text/csv, application/octet-stream; q=0.9, */*; q=0.1",
        }) as resp:
            log.debug("IOSCO response: status=%s content-type=%s", resp.status_code, resp.headers.get("Content-Type"))
            resp.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)
    except Exception:
        log.exception("Failed to download IOSCO CSV")
        raise
    log.info("Saved IOSCO CSV: %s (bytes=%s)", out_path, out_path.stat().st_size)
    return out_path

def parse_csv_url_info(csv_path: Path) -> List[str]:
    """Parse URLs from the given CSV file.
    Return (url, nca_id, nca_jurisdiction, nca_name, validation_date)"""
    urls: map[str] = ()
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for key in ("url", "URL"):
                if key in row and row[key]:
                    try:
                        url = normalize_url(row[key])
                        urls[url] = (int(row.get("nca_id",0)),
                                     row.get("nca_jurisdiction",""),
                                     row.get("nca_name",""),
                                     row.get("validation_date",""))
                    except Exception as e:
                        log.error(f"Error occurred when parsing url: {e}")
                        continue
    return urls
