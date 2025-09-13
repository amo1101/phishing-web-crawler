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
from .wayback import cdx_latest_snapshots_for_domain
from .pywb_mgr import ensure_collection


def parse_csv_urls(csv_path: Path) -> List[str]:
    urls: List[str] = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for key in ("url", "other_urls"):
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

    # 2) Extract URLs & group by domain
    urls = parse_csv_urls(csv_path)
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

    # Build seeds list per domain (from the URLs we just saw)
    seeds_by_domain: Dict[str, List[str]] = {}
    for u in urls:
        d = registrable_domain(u)
        seeds_by_domain.setdefault(d, []).append(u)

    # LIVE DOMAINS
    for domain, status in domain_status.items():
        if status != "live":
            continue
        job_name = f"live-{domain.replace('.', '-')}"
        if heri.job_exists(job_name):
            continue
        seeds = seeds_by_domain.get(domain, [])
        if seeds:
            heri.create_or_update_live_job(domain, seeds, cfg["heritrix"])

    # DEAD DOMAINS
    if cfg["dead_site_mode"] == "heritrix_wayback":
        for domain, status in domain_status.items():
            if status != "dead":
                continue
            stamps = cdx_latest_snapshots_for_domain(
                domain=domain,
                n=cfg["wayback"]["snapshots_per_domain"],
                cdx_endpoint=cfg["wayback"]["cdx_endpoint"],
                base_params=cfg["wayback"]["cdx_params"],
                rps=cfg["wayback"]["rps"]
            )
            # Only create jobs if none exist for this domain
            if any(heri.job_exists(f"wb-{domain.replace('.', '-')}-{ts}") for ts in stamps):
                continue
            heri.create_wayback_job(domain, stamps, cfg["heritrix"])
            st.record_wayback_timestamps(domain, stamps)

    # If/when you switch to 'pywb_record', implement the local recording path here.

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
            # Daily run
            wait_s = _next_daily_time(cfg["schedule"]["daily_run_time"])
            time.sleep(wait_s)
            run_once(cfg, st)

            # After daily run, also (re)launch due Heritrix jobs by cadence
            due_domains = st.get_domains_due_for_heritrix(cfg["schedule"]["heritrix_job_interval_days"])
            if due_domains:
                # Minimal: just trigger live jobs again (we’ll reuse seeds file from last run)
                heri = Heritrix(
                    base_url=cfg["heritrix"]["base_url"],
                    username=cfg["heritrix"]["username"],
                    password=cfg["heritrix"]["password"],
                    jobs_dir=cfg["heritrix"]["jobs_dir"],
                    tls_verify=cfg["heritrix"]["tls_verify"]
                )
                for d in due_domains:
                    # (Re)build+launch by job name if needed; simplest is to build & launch again:
                    job_name = f"live-{d.replace('.', '-')}"
                    try:
                        heri.build_job(job_name)
                        heri.launch_job(job_name)
                        st.mark_heritrix_launch(d)
                    except Exception:
                        # silently continue; we could log this
                        pass
        except Exception:
            # In systemd, we restart on any crash; you can add logging here
            time.sleep(5)
