import sys
from playwright.sync_api import sync_playwright
from pathlib import Path
from __future__ import annotations
from pathlib import Path
from datetime import date,datetime
from typing import Optional
import logging, requests
from typing import Dict, List, Tuple, Set
from pathlib import Path
import urlextract
import pandas as pd
from .normalize import normalize_url, registrable_domain, url_start_with_domain
import logging

logging.basicConfig(level=logging.INFO)

ID_COL = "id"
NCA_ID_COL = "nca_id"
NCA_JURIS_COL = "nca_jurisdiction"
NCA_NAME_COL = "nca_name"
NCA_URL_COL = "nca_url"
VALIDATION_DATE_COL = "validation_date"
URL_COL = "url"
COMNAME_COL = "commercial_name"
ADDINFO_COL = "additional_information"
OTHERURL_COL = "other_urls"

# Based on Dave's code from safe-browsing.py, with some adjustments.
extractor = urlextract.URLExtract()
def parse_url_field(urlField: str) -> list:
    return extractor.find_urls(urlField)

# Deprecated!! now IOSCO blocks non-human downloading of csv
# so we have to use playwright to mimic browser behaviour
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

    logging.info("Fetching IOSCO CSV: %s params=%s timeout=%ss -> %s", base_url, params, timeout, out_path)
    try:
        with requests.get(base_url, params=params, timeout=timeout, stream=True, headers={
            "User-Agent": "Mozilla/5.0 (compatible; FMA-Crawler/1.0)",
            "Accept": "text/csv, application/octet-stream; q=0.9, */*; q=0.1",
        }) as resp:
            logging.debug("IOSCO response: status=%s content-type=%s", resp.status_code, resp.headers.get("Content-Type"))
            resp.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)
    except Exception:
        logging.exception("Failed to download IOSCO CSV")
        raise
    logging.info("Saved IOSCO CSV: %s (bytes=%s)", out_path, out_path.stat().st_size)
    return out_path

# use playwright to mimic browser behaviour and download csv
def fetch_with_playwright(output: Path):
    with sync_playwright() as p:

        # --- Anti-detection Chromium Launch ---
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--window-size=1280,800",
            ]
        )

        # --- Spoof a REAL Chrome browser ---
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            device_scale_factor=1,
            is_mobile=False,
            java_script_enabled=True,
        )

        page = context.new_page()

        logging.info("Opening page...")
        page.goto("https://www.iosco.org/i-scan")
        page.wait_for_load_state("networkidle")

        logging.info("Page loaded. Locating button...")

        # The Export button *will* appear now
        button = page.locator("button:has-text('Export to CSV')")
        button.wait_for(state="visible")

        logging.info("Button found. Clicking...")

        with context.expect_page() as popup_info:
            button.click()

        popup = popup_info.value
        popup.wait_for_load_state("networkidle")
        logging.info("Popup opened.")

        csv_url = popup.url
        logging.info(f"CSV URL = {csv_url}")

        # --- Fetch CSV using request API (robust) ---
        req = p.request.new_context()

        logging.info("Downloading CSV...")
        resp = req.get(csv_url)

        if not resp.ok:
            raise RuntimeError(f"Download failed: {resp.status}")

        csv_file = output / 'iosco_export.csv'
        Path(csv_file).write_bytes(resp.body())
        logging.info("CSV saved successfully.")

        browser.close()
        logging.info("Done.")
        return csv_file

# tidy up raw URLs
def tidy_raw_url(rawURL: str) -> str:
    # custom data cleaning for observed errors in IOSCO URL data
    tidyURL = rawURL
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
    30231: ['https://www.gmtdirect.com', 'https://www.gmtplatform.com'],
    28662: ['https://panel.billionaire-trade.co.com', 'https://trading.billionaire-trade.co.com'],
    12828: ['https://secure.capitalgmafx.com', 'https://trade.capitalgmafx.com', 'https://www.marketscfds.com', 'https://secure.marketscfds.com', 'https://ztrade24.com', 'https://secure.ztrade24.com']
}

def parse_url_cols(row) -> list:
    def unique_by_domain(urls: list) -> list:
        # de-dup URLs by their registrable domain + path,
        # to avoid duplicates caused by minor variations (e.g. http vs https, www vs non-www, trailing slash, etc.)
        seen = set()
        unique_urls = []
        for url in urls:
            u = url_start_with_domain(url)
            if u not in seen:
                seen.add(u)
                unique_urls.append(url)
        return unique_urls

    all_urls = []
    nca_url = parse_url_field(str(getattr(row, NCA_URL_COL)))
    nca_domain = registrable_domain(nca_url[0]) if nca_url else None
    # url column
    all_urls.extend(parse_url_field(str(getattr(row, URL_COL))))
    #commercial_name column
    all_urls.extend(parse_url_field(str(getattr(row, COMNAME_COL))))
    #addInfoCol column
    all_urls.extend(parse_url_field(str(getattr(row, ADDINFO_COL))))
    # otherurlCol column, urls are separated by '|'
    otherurls = getattr(row, OTHERURL_COL)
    list_otherurls = str(otherurls).split("|")
    for url in list_otherurls:
        all_urls.extend(parse_url_field(str(url)))
    # filter out those are under the nca domain, as those are likely to be false positives (e.g. regulator's own website)
    if nca_domain:
        all_urls = [url for url in all_urls if registrable_domain(url) != nca_domain]
    return unique_by_domain(all_urls)

def parse_csv_url_info(csv_path: Path):
    """Parse URLs from the given CSV file, store them into csv"""
    urls: dict[str, Tuple[int, str, str, str]] = {}
    try:
        csv_df = pd.read_csv(csv_path, dtype=str, low_memory=False)
        for row in csv_df.itertuples():
            id = getattr(row, ID_COL)
            if id in manual_fixes:
                url_list = manual_fixes[id]
            else:
                url_list = parse_url_cols(row)
            nca_id = int(getattr(row, NCA_ID_COL))
            nca_jurisdiction = getattr(row, NCA_JURIS_COL)
            nca_name = getattr(row, NCA_NAME_COL)
            validation_date = getattr(row, VALIDATION_DATE_COL)
            attrs = (nca_id, nca_jurisdiction, nca_name, validation_date)
            for url in url_list:
                tidyURL = tidy_raw_url(url)
                urls[tidyURL] = attrs
    except pd.errors.EmptyDataError:
        logging.warning("CSV file is empty: %s", csv_path)
        return {}
    except Exception:
        logging.exception("Failed to read CSV: %s", csv_path)
        raise

    logging.info(f'Total urls parsed ({len(urls)}):')
    
    # Write results to CSV file
    output_df = pd.DataFrame([
        {'url': url, NCA_ID_COL: attrs[0], NCA_JURIS_COL: attrs[1], 
         NCA_NAME_COL: attrs[2], VALIDATION_DATE_COL: attrs[3]}
        for url, attrs in urls.items()
    ])
    output_df.to_csv(csv_path.parent / 'clean_urls.csv', index=False)
    logging.info('Clean URLs saved to clean_urls.csv')

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 fetch_iosco_csv.py <base_dir>")
        sys.exit(1)

    base_dir = sys.argv[1]
    try:
        output_today = Path(base_dir) / f"{datetime.now().strftime('%Y%m%d')}"
        output_today.mkdir(parents=True, exist_ok=True)
        csv_file = fetch_with_playwright(output_today)
        parse_csv_url_info(csv_file)
    except Exception as e:
        logging.error(f"An error occurred {e}")
