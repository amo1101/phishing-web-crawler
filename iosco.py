from __future__ import annotations
from pathlib import Path
from datetime import date
from typing import Optional
import requests

def fetch_iosco_csv(
    csv_root: Path,
    start_date: Optional[date],
    end_date: Optional[date],
    *,
    nca_id: int = 64,
    subsection: str = "main",
    timeout: int = 60
) -> Path:
    """
    Download IOSCO I-SCAN CSV:
      - First run: start_date and end_date are None -> full export.
      - Incremental run: include ValidationDateStart/End with YYYY-MM-DD strings.
    Saves under csv_root and returns the file path.
    """
    # 1) Ensure target dir
    csv_root.mkdir(parents=True, exist_ok=True)

    # 2) Build file name
    if start_date and end_date:
        csv_name = f"iosco_export_{start_date.isoformat()}_to_{end_date.isoformat()}.csv"
    else:
        csv_name = f"iosco_export_full_{date.today().isoformat()}.csv"
    out_path = csv_root / csv_name

    # 3) Build URL & params safely
    base_url = "https://www.iosco.org/i-scan/"
    url = base_url + "?export-to-csv"
    params = {
        "SUBSECTION": subsection,
        "NCA_ID": str(nca_id),
    }
    if start_date and end_date:
        # Most servers accept ISO-8601 YYYY-MM-DD; adjust here if the site requires another format
        params["ValidationDateStart"] = start_date.isoformat()
        params["ValidationDateEnd"]   = end_date.isoformat()

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; Phishing-Web-Crawler/1.0)",
        "Accept": "text/csv, application/octet-stream; q=0.9, */*; q=0.1",
    }

    # 4) Request with timeout & streaming
    with requests.get(url, params=params, headers=headers, timeout=timeout, stream=True) as resp:
        resp.raise_for_status()
        # 5) Write to disk
        with open(out_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)

    return out_path
