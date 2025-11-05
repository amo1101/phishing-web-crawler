from __future__ import annotations
from datetime import datetime, date, time as dtime, timedelta, timezone
import time
from typing import Dict, List, Tuple, Set
from pathlib import Path
from .config import Config
from .state import State
from .iosco import fetch_iosco_csv, parse_csv_url_info
from .liveness import classify_urls
from .jobqueue import LIVE_CRAWL, WAYBACK_DOWNLOAD

import logging
log = logging.getLogger(__name__)

def run_once(cfg: Config, st: State):
    """Run one ingestion cycle: fetch CSV, parse URLs, classify liveness, enqueue jobs."""
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
    url_info = parse_csv_url_info(csv_path)
    urls = list(url_info)
    existing_urls = st.fetch_all_urls()
    log.info("Parsed %d URLs, existing URLs: %d", len(urls), len(existing_urls))
    new_urls = list(set(urls) - set(existing_urls))
    log.info("New urls to crawl %d", len(new_urls))

    # 3) URL liveness check
    url_status = classify_urls(
        urls=list(new_urls),
        timeout=cfg["liveness"]["timeout_seconds"],
        treat_4xx_as_live=cfg["liveness"]["treat_http_4xx_as_live"],
        max_workers=cfg["liveness"]["max_parallel_probes"],
    )

    # 4) create crawling job or wayback download job
    for url, status in url_status.items():
        job_type = LIVE_CRAWL
        job_desc = 'Live'
        job_priority = 50
        nca_id, nca_jurisdiction, nca_name, validate_date = url_info[url]
        if status == "dead":
            job_type = WAYBACK_DOWNLOAD
            job_desc = "wayback download"
            job_priority = 100

        st.add_nca(nca_id, nca_jurisdiction, nca_name)
        st.enqueue_job_unique(job_type, url, nca_id, validate_date, job_priority)
        log.info("Enqueued %s job for %s", job_desc, url)

def _next_daily_time(local_hhmm: str) -> float:
    return 0
    # returns seconds until next occurrence of local_hhmm
    hh, mm = map(int, local_hhmm.split(":"))
    now = datetime.now()
    target = datetime.combine(now.date(), dtime(hh, mm))
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()

def run_loop(cfg: Config, st: State):
    """Run the daily ingestion loop."""
    while True:
        try:
            # Wait until next daily time
            wait_s = _next_daily_time(cfg["schedule"]["daily_run_time"])
            time.sleep(wait_s)

            # Daily ingestion -> enqueue jobs
            run_once(cfg, st)

            # Sleep a short period before recalculating
            time.sleep(3600)

        except Exception as e:
            log.error('An exception occurred: %s', str(e))
            time.sleep(5)
