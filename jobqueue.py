# fma/jobqueue.py
from __future__ import annotations
import threading
import time
from typing import Dict, List
from datetime import datetime
from .state import State
from .heritrix import Heritrix

# Queue job types
LIVE_CREATE = "LIVE_CREATE"
LIVE_RELAUNCH = "LIVE_RELAUNCH"
WAYBACK_CREATE = "WAYBACK_CREATE"

class JobQueueWorker:
    def __init__(self, cfg, state: State):
        self.cfg = cfg
        self.state = state
        self.heri = Heritrix(
            base_url=cfg["heritrix"]["base_url"],
            username=cfg["heritrix"]["username"],
            password=cfg["heritrix"]["password"],
            jobs_dir=cfg["heritrix"]["jobs_dir"],
            tls_verify=cfg["heritrix"]["tls_verify"]
        )
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    # -- mapping from job type -> handler
    def _handle_job(self, job) -> None:
        jtype = job["type"]
        domain = job["domain"]
        payload = job["payload"] or {}

        if jtype == LIVE_CREATE:
            name = self.heri.create_or_update_live_job(domain, payload.get("seeds", []), self.cfg["heritrix"])
            payload["job_names"] = [name]; self.state.update_job_payload(job["id"], payload)

        elif jtype == LIVE_RELAUNCH:
            jn = f"live-{domain.replace('.', '-')}"
            self.heri.build_job(jn); self.heri.launch_job(jn)
            payload["job_names"] = [jn]; self.state.update_job_payload(job["id"], payload)

        elif jtype == WAYBACK_CREATE:
            ts = payload["timestamp"]; url_seeds = payload["url_seeds"]
            name = self.heri.create_wayback_job_with_seeds(domain, ts, url_seeds, self.cfg["heritrix"])
            payload["job_names"] = [name]; self.state.update_job_payload(job["id"], payload)

        else:
            self.state.mark_skipped(job["id"], f"unknown job type {jtype}")

    def _reconcile(self):
        """Poll Heritrix for RUNNING jobs and mark SUCCEEDED when all underlying job_names are no longer RUNNING."""
        running = self.state.list_running_jobs()
        for job in running:
            payload = job.get("payload") or {}
            job_names = payload.get("job_names") or []
            if not job_names:
                # No way to reconcile; mark as SUCCEEDED after a grace period?
                self.state.mark_succeeded(job["id"])
                continue

            statuses = [self.heri.get_job_status(name) for name in job_names]
            # Consider SUCCEEDED when none are RUNNING (Heritrix often reports FINISHED/PAUSED/ENDED)
            if all(s not in ("RUNNING", "UNBUILT") for s in statuses):
                self.state.mark_succeeded(job["id"])

    def run_forever(self):
        max_parallel = int(self.cfg["queue"]["max_parallel_jobs"])
        reconcile_every = int(self.cfg["queue"]["reconcile_interval_seconds"])
        max_retries = int(self.cfg["queue"]["max_retries"])
        backoff = int(self.cfg["queue"]["retry_backoff_seconds"])

        last_reconcile = 0
        while not self._stop.is_set():
            now = time.time()

            # 1) Reconcile periodically
            if now - last_reconcile >= reconcile_every:
                try:
                    self._reconcile()
                except Exception:
                    pass
                last_reconcile = now

            # 2) Check how many RUNNING
            running_count = self.state.count_running_jobs()
            capacity = max_parallel - running_count
            if capacity <= 0:
                time.sleep(1)
                continue

            # 3) Fetch up to 'capacity' pending jobs and start them
            jobs = self.state.fetch_next_pending(limit=capacity)
            if not jobs:
                time.sleep(1)
                continue

            for job in jobs:
                # mark RUNNING first (so concurrency is accurate)
                self.state.mark_running(job["id"])
                try:
                    self._handle_job(job)
                    # leave as RUNNING: reconcile loop will mark SUCCEEDED when underlying jobs finish
                except Exception as ex:
                    self.state.mark_failed(job["id"], str(ex), max_retries=max_retries)
                    # if it was put back to PENDING, wait a bit to avoid tight loop
                    time.sleep(backoff)
