from __future__ import annotations
from datetime import datetime, date, time as dtime, timedelta, timezone
import time
from typing import Dict, List, Tuple, Set, Optional
from pathlib import Path
from .config import Config
from .state import State
from .jobqueue import LIVE_CRAWL, WAYBACK_DOWNLOAD
import traceback
import pandas as pd

import logging
log = logging.getLogger(__name__)

# relies on daily task to download and parse urls and check liveness
def get_iosco_urls(
    csv_root: Path,
    start_date: Optional[date],
    end_date: Optional[date],
    *,
    nca_id: str = ""
):
    output_today = Path(csv_root) / f"{datetime.now().strftime('%Y%m%d')}"
    if not output_today.exists():
        return {}
    url_df = pd.read_csv(output_today / 'clean_urls.csv')
    url_df["validation_date"] = pd.to_datetime(url_df["validation_date"],
                                               format="%Y-%m-%d",
                                               errors="coerce").dt.date
    if start_date and end_date:
        url_df = url_df.query('validation_date >= @start_date and validation_date <= @end_date')
    if nca_id:
        url_df = url_df.query('nca_id == @nca_id')
    return url_df.set_index('url').to_dict('index')

def run_once(cfg: Config, st: State):
    """Run one ingestion cycle: fetch URLs, enqueue jobs."""
    now = datetime.now(timezone.utc)
    log.info("Daily run started at %s", now.isoformat())

    # 1) Decide full vs incremental
    last_full = st.get_last_full_run()
    last_incr = st.get_last_incremental_run()
    csv_root = Path(cfg["iosco"]["csv_root"])
    url_info = {}

    # Testing: force full run
    #last_full = None
    if last_full is None:
        # First full run from base_date
        url_info = get_iosco_urls(
            csv_root=csv_root,
            start_date=datetime.strptime(cfg["schedule"]["base_date"], '%Y-%m-%d').date(),
            end_date=now.date(),
            nca_id=cfg["iosco"]["nca_id"]
        )
        st.set_last_full_run(now)
    else:
        # Incremental from last_incremental_run (or last_full if no incr yet) to now
        start = last_incr or last_full
        url_info = get_iosco_urls(
            csv_root=csv_root,
            start_date=start.date(),
            end_date=now.date(),
            nca_id=cfg["iosco"]["nca_id"]
        )
        st.set_last_incremental_run(now)

    url_info_filtered = {k:v for k,v in url_info.items() if not st.check_url_exists(k)}
    total_urls = len(url_info_filtered)
    live_urls = sum(1 for v in url_info_filtered.values() if str(v.get("liveness", "")).lower() == "live")
    dead_urls = sum(1 for v in url_info_filtered.values() if str(v.get("liveness", "")).lower() == "dead")
    log.info(f"Total URLs to process: {total_urls}, live: {live_urls}, dead: {dead_urls}")

    # 2) create crawling job or wayback download job
    for k, v in url_info_filtered.items():
        job_type = LIVE_CRAWL
        job_desc = 'Live'
        job_priority = 50
        nca_id = v['nca_id']
        liveness = v['liveness']
        nca_jurisdiction = v['nca_jurisdiction']
        nca_name = v['nca_name']
        validate_date = v['validation_date'].isoformat()

        if liveness == 'dead':
            job_type = WAYBACK_DOWNLOAD
            job_desc = "wayback download"
            job_priority = 100

        st.add_nca(nca_id, nca_jurisdiction, nca_name)
        st.enqueue_job_unique(job_type, k, nca_id, validate_date, job_priority)
        log.info("Enqueued %s job for %s", job_desc, k)

def _next_daily_time(local_hhmm: str) -> float:
    # returns seconds until next occurrence of local_hhmm
    #return 0
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
            time.sleep(600)
        except Exception as e:
            log.error('An exception occurred: %s', str(e))
            traceback.print_exc()
            time.sleep(5)
