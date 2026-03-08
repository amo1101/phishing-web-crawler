from __future__ import annotations
from pathlib import Path
from datetime import date
from typing import Optional
import logging, requests
from typing import Dict, List, Tuple, Set
from pathlib import Path
import validators
import urllib
import urlextract
import pandas as pd
from .normalize import normalize_url, registrable_domain

log = logging.getLogger(__name__)

ID_COL = "id"
NCA_ID_COL = "nca_id"
NCA_JURIS_COL = "nca_jurisdiction"
NCA_NAME_COL = "nca_name"
NCA_URL_COL = "nca_url"
VALIDATION_DATE_COL = "validation_date"
URL_COL = "url"
COMNAME_COL = "commercial_name"
ADDINFO_COL = "additional_info"
OTHERURL_COL = "other_urls"

def fetch_iosco_csv(
    csv_root: Path,
    start_date: Optional[date],
    end_date: Optional[date],
    *,
    nca_id: str = "",
    subsection: str = "main",
    timeout: int = 60
) -> Path:
    csv_root.mkdir(parents=True, exist_ok=True)
    if start_date and end_date:
        csv_name = f"iosco_export_{start_date.isoformat()}_to_{end_date.isoformat()}.csv"
    else:
        csv_name = f"iosco_export_all_{date.today().isoformat()}.csv"
    out_path = csv_root / csv_name

    base_url = "https://www.iosco.org/i-scan/?export-to-csv"
    params = {"SUBSECTION": subsection}
    if nca_id != "":
        params["NCA_ID"] = nca_id
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

# Based on Dave's code from safe-browsing.py, with some adjustments.
extractor = urlextract.URLExtract()
def parse_url_field(urlField: str) -> list:
    return extractor.find_urls(urlField)

# tidy up raw URLs
def tidy_raw_url(rawURL: str) -> str:
    # custom data cleaning for observed errors in IOSCO URL data
    if tidyURL.startswith("ttps://"):
        tidyURL = "h" + tidyURL
    if tidyURL.startswith("www.https://"):
        tidyURL = tidyURL[4:]
    if tidyURL.startswith("https://."):
        tidyURL = "https://" + tidyURL[9:]
    if tidyURL.startswith("htttps://"):
        tidyURL = "https://" + tidyURL[9:]
    if tidyURL.startswith("httops://"):
        tidyURL = "https://" + tidyURL[9:]
    if tidyURL.startswith("htpps://"):
        tidyURL = "https://" + tidyURL[8:]
    if tidyURL.startswith("https://www.."):
        tidyURL = "https://www." + tidyURL[13:]  
    if tidyURL.startswith("pagehttps://"):   
        tidyURL = tidyURL[4:]
    if tidyURL.startswith("pageshttps://"):   
        tidyURL = tidyURL[5:]
    if tidyURL.startswith("websitehttps://"):   
        tidyURL = tidyURL[7:]
    if tidyURL.startswith("websiteshttps://"):   
        tidyURL = tidyURL[8:]
    if tidyURL.startswith("andhttps://"):   
        tidyURL = tidyURL[3:]
    return normalize_url(tidyURL)

# manual data cleaning fixes
manual_fixes = {
    30231: {'https://www.gmtdirect.com', 'https://www.gmtplatform.com'},
    28662: {'https://panel.billionaire-trade.co.com', 'https://trading.billionaire-trade.co.com'},
    12828: {'https://secure.capitalgmafx.com', 'https://trade.capitalgmafx.com', 'https://www.marketscfds.com', 'https://secure.marketscfds.com', 'https://ztrade24.com', 'https://secure.ztrade24.com'}
}

def parse_url_cols(row) -> set:
    all_urls = set()
    nca_url = parse_url_field(str(getattr(row, NCA_URL_COL)))
    nca_domain = registrable_domain(nca_url[0]) if nca_url else None
    # url column
    all_urls.update(parse_url_field(str(getattr(row, URL_COL))))
    #commercial_name column
    all_urls.update(parse_url_field(str(getattr(row, COMNAME_COL))))
    #addInfoCol column
    all_urls.update(parse_url_field(str(getattr(row, ADDINFO_COL))))
    # otherurlCol column, urls are separated by '|'
    otherurls = getattr(row, OTHERURL_COL)
    list_otherurls = str(otherurls).split("|")
    for url in list_otherurls:
        all_urls.update(parse_url_field(str(url)))
    # filter out that are under the nca domain, as those are likely to be false positives (e.g. regulator's own website)
    if nca_domain:
        all_urls = {url for url in all_urls if registrable_domain(url) != nca_domain}
    return all_urls

def parse_csv_url_info(csv_path: Path) -> map[str, Tuple[str, str, str, str]]:
    """Parse URLs from the given CSV file.
    Return (url, nca_id, nca_jurisdiction, nca_name, validation_date)"""
    urls: map[str, Tuple[int, str, str, str]] = {}
    try:
        csv_df = pd.read_csv(csv_path)
        for row in csv_df.itertuples():
            id = getattr(row, ID_COL)
            if id in manual_fixes:
                url_set = manual_fixes[id]
            else:
                url_set = parse_url_cols(row)
            nca_id = getattr(row, NCA_ID_COL)
            nca_jurisdiction = getattr(row, NCA_JURIS_COL)
            nca_name = getattr(row, NCA_NAME_COL)
            validation_date = getattr(row, VALIDATION_DATE_COL)
            attrs = (nca_id, nca_jurisdiction, nca_name, validation_date)
            for url in url_set:
                tidyURL = tidy_raw_url(url)
                urls[tidyURL] = attrs
    except Exception:
        log.exception("Failed to read CSV: %s", csv_path)
        raise
  
    return urls
