from __future__ import annotations
from datetime import datetime, date, time as dtime, timedelta, timezone
import time
from typing import Dict, List, Tuple, Set
from pathlib import Path
import csv

from .config import Config
from .state import State
from .normalize import normalize_url, registrable_domain
from .iosco import fetch_iosco_csv
from .liveness import classify_urls
from .jobqueue import LIVE_CRAWL, WAYBACK_DOWNLOAD

import logging
log = logging.getLogger(__name__)


def parse_csv_urls(csv_path: Path) -> List[str]:
    urls: List[str] = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for key in ("url", "URL", "other_urls"):
                if key in row and row[key]:
                    for u in row[key].split('|'):
                        urls.append(normalize_url(u))
    return urls[:30]

def run_once(cfg: Config, st: State):
    now = datetime.now(timezone.utc)
    log.info("Daily run started at %s", now.isoformat())

    # 1) Decide full vs incremental
    last_full = st.get_last_full_run()
    last_incr = st.get_last_incremental_run()
    csv_root = Path(cfg["iosco"]["csv_root"])

    if last_full is None:
        # First full run from base_date
        csv_path = fetch_iosco_csv(
            csv_root=csv_root,
            start_date=datetime.strptime(cfg["schedule"]["base_date"], '%Y-%m-%d').date(),
            end_date=now.date(),
            nca_id=int(cfg["iosco"]["nca_id"]),
            subsection=cfg["iosco"]["subsection"],
            timeout=int(cfg["iosco"]["request_timeout_seconds"])
        )
        st.set_last_full_run(now)
    else:
        # Incremental from last_incremental_run (or last_full if no incr yet) to now
        start = last_incr or last_full
        csv_path = fetch_iosco_csv(
            csv_root=csv_root,
            start_date=start.date(),
            end_date=now.date(),
            nca_id=int(cfg["iosco"]["nca_id"]),
            subsection=cfg["iosco"]["subsection"],
            timeout=int(cfg["iosco"]["request_timeout_seconds"])
        )
        st.set_last_incremental_run(now)

    # 2) Extract URLs and filter existing urls
    log.info("CSV path %s", csv_path)
    urls = set(parse_csv_urls(csv_path)) - set(st.fetch_all_urls())
    log.info("Parsed %d URLs from CSV %s", len(urls), csv_path)

    # 3) URL liveness check
    url_status = classify_urls(
        urls=list(urls),
        timeout=cfg["liveness"]["timeout_seconds"],
        treat_4xx_as_live=cfg["liveness"]["treat_http_4xx_as_live"],
        max_workers=cfg["liveness"]["max_parallel_probes"],
    )

    # 4) create crawling job or wayback download job
    for url, status in url_status.items():
        job_type = LIVE_CRAWL
        job_desc = 'Live'
        job_priority = 100
        if status == "dead":
            job_type = WAYBACK_DOWNLOAD
            job_priority = 50

        st.enqueue_job_unique(job_type, url, job_priority)
        log.info("Enqueued %s job for %s", job_desc, url)

def _next_daily_time(local_hhmm: str) -> float:
    # returns seconds until next occurrence of local_hhmm
    hh, mm = map(int, local_hhmm.split(":"))
    now = datetime.now()
    target = datetime.combine(now.date(), dtime(hh, mm))
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()

def run_loop(cfg: Config, st: State):
    while True:
        try:
            # Wait until next daily time
            wait_s = _next_daily_time(cfg["schedule"]["daily_run_time"])
            time.sleep(wait_s)

            # Daily ingestion -> enqueue jobs
            run_once(cfg, st)

            # Sleep a short period before recalculating
            time.sleep(3600)

        except Exception:
            time.sleep(5)
