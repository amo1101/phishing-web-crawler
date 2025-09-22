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
from .liveness import classify_domains
from .heritrix import Heritrix
from .wayback import cdx_latest_snapshots_for_url
from .pywb_mgr import ensure_collection
from .jobqueue import LIVE_CREATE, WAYBACK_CREATE, LIVE_RELAUNCH


def parse_csv_urls(csv_path: Path) -> List[str]:
    urls: List[str] = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for key in ("url", "URL", "other_urls"):
                if key in row and row[key]:
                    for u in row[key].split('|'):
                        urls.append(normalize_url(u))
    return urls

def run_once(cfg: Config, st: State):
    now = datetime.now(timezone.utc)

    # 1) Decide full vs incremental
    last_full = st.get_last_full_run()
    last_incr = st.get_last_incremental_run()
    csv_root = Path(cfg["iosco"]["csv_root"])

    if last_full is None:
        # First run: full export
        csv_path = fetch_iosco_csv(
            csv_root=csv_root,
            start_date=None,
            end_date=None,
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

    print(f"csv_path: {csv_path}")
    # 2) Extract URLs & group by domain
    urls = parse_csv_urls(csv_path)
    print(f"urls: {urls}")
    seen_at = now
    for u in urls:
        d = registrable_domain(u)
        st.upsert_url(u, d, seen_at)

    # 3) Liveness by domain
    domain_status = classify_domains(
        urls=list(set(urls)),
        timeout=cfg["liveness"]["timeout_seconds"],
        treat_4xx_as_live=cfg["liveness"]["treat_http_4xx_as_live"],
        max_workers=cfg["liveness"]["max_parallel_probes"],
    )
    for d, s in domain_status.items():
        st.set_domain_status(d, s)
    
    print(f"domain_status: {domain_status}")

    # 4) Ensure Pywb collection exists
    ensure_collection(cfg["pywb"]["collection"], cfg["pywb"]["wb_manager_bin"])

    # 5) Orchestrate per-domain work
    heri = Heritrix(
        base_url=cfg["heritrix"]["base_url"],
        username=cfg["heritrix"]["username"],
        password=cfg["heritrix"]["password"],
        jobs_dir=cfg["heritrix"]["jobs_dir"],
        tls_verify=cfg["heritrix"]["tls_verify"]
    )

    # seeds_by_domain from provided URLs
    seeds_by_domain: Dict[str, List[str]] = {}
    for u in urls:
        d = registrable_domain(u)
        seeds_by_domain.setdefault(d, []).append(u)

    print(f"seeds_by_domain: {seeds_by_domain}")

    # Live: create or append seeds
    for domain, status in domain_status.items():
        if status != "live":
            continue
        job_name = f"live-{domain.replace('.', '-')}"
        domain_seeds = sorted(set(seeds_by_domain.get(domain, [])))
        if not domain_seeds:
            continue
        print(f"job_name: {job_name}, domain_seeds {domain_seeds}")
        if heri.job_exists(job_name):
            heri.append_seeds(job_name, domain_seeds)  # ActionDirectory
        else:
            st.enqueue_job_unique(LIVE_CREATE, domain, {"seeds": domain_seeds}, priority=50)

    # Dead: per-URL CDX; group seeds per (domain, timestamp)
    seeds_by_d_ts: Dict[tuple[str,str], set[str]] = {}
    for u in urls:
        d = registrable_domain(u)
        if domain_status.get(d) != "dead":
            continue
        stamps = cdx_latest_snapshots_for_url(
            url=u,
            n=cfg["wayback"]["snapshots_per_domain"],
            cdx_endpoint=cfg["wayback"]["cdx_endpoint"],
            base_params=cfg["wayback"]["cdx_params"],
            rps=cfg["wayback"]["rps"]
        )
        for ts in stamps:
            seeds_by_d_ts.setdefault((d, ts), set()).add(u)

    for (d, ts), urlset in seeds_by_d_ts.items():
        job_name = f"wb-{d.replace('.', '-')}-{ts}"
        if heri.job_exists(job_name):
            continue
        st.enqueue_job_unique(WAYBACK_CREATE, d, {"timestamp": ts, "url_seeds": sorted(urlset)}, priority=60)

def _next_daily_time(local_hhmm: str) -> float:
    # returns seconds until next occurrence of local_hhmm
    hh, mm = map(int, local_hhmm.split(":"))
    now = datetime.now()
    target = datetime.combine(now.date(), dtime(hh, mm))
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()

def enqueue_cadence_relaunches(cfg: Config, st: State):
    # Only relaunch live domains according to cadence; skip wayback
    due_domains = st.get_domains_due_for_heritrix(cfg["schedule"]["heritrix_job_interval_days"])
    from .jobqueue import LIVE_RELAUNCH
    for d in due_domains:
        st.enqueue_job_unique(LIVE_RELAUNCH, d, {}, priority=70)

def run_loop(cfg: Config, st: State):
    while True:
        try:
            # Wait until next daily time
            wait_s = _next_daily_time(cfg["schedule"]["daily_run_time"])
            time.sleep(wait_s)

            # Daily ingestion -> enqueue jobs
            run_once(cfg, st)

            # Enqueue relaunches for due live domains
            enqueue_cadence_relaunches(cfg, st)

            # Sleep a short period before recalculating
            time.sleep(5)

        except Exception:
            # Log if you add logging; keep process alive for systemd
            time.sleep(5)
