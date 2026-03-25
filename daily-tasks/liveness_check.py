import sys
from __future__ import annotations
import socket
from pathlib import Path
import concurrent.futures as cf
from typing import Tuple, Dict, List
from urllib.parse import urlsplit
import requests
import logging
from datetime import datetime
import pandas as pd

logging.basicConfig(level=logging.INFO)

def resolve_host(host: str) -> bool:
    try:
        socket.getaddrinfo(host, None)
        return True
    except Exception as e:
        logging.warning("Skip, DNS resolution failed for host: %s, exception: %s", host, str(e))
        return False

def probe_url(url: str, timeout: int) -> Tuple[str, str]:
    """
    Returns (url, 'live'|'dead')
    """
    try:
        parts = urlsplit(url)
        if not parts.netloc:
            logging.debug('no netloc')
            return url, "dead"
        if not resolve_host(parts.hostname):
            logging.debug('resolve host failed')
            return url, "dead"
        # Try HEAD; fallback GET
        r = requests.head(url, allow_redirects=True, timeout=timeout)
        logging.debug("HEAD %s -> %s", url, r.status_code)
        if r.status_code >= 500:
            logging.debug("HEAD %s -> network error", url)
            return url, "dead"
        return url, "live"
    except requests.RequestException:
        return url, "dead"

def classify_urls(urls: List[str], timeout: int, treat_4xx_as_live: bool=True, max_workers: int = 30) -> Dict[str, str]:
    """
    Returns {url: 'live'|'dead'} based on the *best* observed status among its URLs.
    """
    # probe
    logging.info("Liveness: probing %d URLs (timeout=%ss, workers=%d)", len(urls), timeout, max_workers)
    results: Dict[str, str] = {}
    with cf.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(probe_url, u, timeout) for u in urls]
        for f in cf.as_completed(futs):
            url, status = f.result()
            results[url] = status

    # 4xx -> live (if configured)
    if treat_4xx_as_live:
        # requests.head already followed redirects and we only labeled 5xx/network as dead.
        pass

    logging.info("Liveness summary: live=%d dead=%d", sum(1 for s in results.values() if s=="live"),
                                               sum(1 for s in results.values() if s=="dead"))
    return results

# check livenss for all URLs daily,create a cron job for this
def check_liveness(base_dir: Path):
    output_today = Path(base_dir) / f"{datetime.now().strftime('%Y%m%d')}"
    if not output_today.exists():
        logging.info('CSV file has not been downloaded today!')
        return
    url_file = output_today + "clean_urls.csv"
    url_df = pd.read_csv(url_file)
    urls = url_df['url'].tolist()
    url_status = classify_urls(urls)
    url_df['is_live'] = url_df['url'].map(url_status)
    url_df.to_csv(url_file, index=False)
    logging.iinfo("Liveness check completed, results saved to %s", url_file)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 liveness_check.py <base_dir>")
        sys.exit(1)

    base_dir = sys.argv[1]
    try:
        check_liveness(Path(base_dir))
    except Exception as e:
        logging.error(f"An error occurred {e}")
