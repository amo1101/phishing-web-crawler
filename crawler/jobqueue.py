# fma/jobqueue.py
from __future__ import annotations
import threading
import time
from typing import Dict, List
from datetime import datetime
from .state import State
from .heritrix import Heritrix
from .wb_downloader import WBDownloader
import logging, time

log = logging.getLogger(__name__)

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
        self.wb_downloader = WBDownloader(
            output_dir=cfg["wayback"]["output_dir"],
            concurrency=int(cfg["wayback"]["concurrency"])
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
            log.info("LIVE_CREATE domain=%s", domain)
            name = self.heri.create_or_update_live_job(domain, payload.get("seeds", []), self.cfg["heritrix"])
            payload["job_names"] = [name]
            self.state.update_job_payload(job["id"], payload)

        elif jtype == LIVE_RELAUNCH:
            log.info("LIVE_RELAUNCH domain=%s", domain)
            name = self.heri.relaunch_job(domain)
            payload["job_names"] = [name]
            self.state.update_job_payload(job["id"], payload)

        elif jtype == WAYBACK_CREATE:
            log.info("WAYBACK_CREATE domain=%s", domain)
            name = self.wb_downloader.create_wayback_job(domain)
            payload["job_names"] = [name]
            self.state.update_job_payload(job["id"], payload)

        else:
            self.state.mark_skipped(job["id"], f"unknown job type {jtype}")
            return

        self.state.mark_launch(domain, jtype)

    def _get_job_status(self, job_type, job_name) -> str:
        if job_type in (LIVE_CREATE, LIVE_RELAUNCH):
            return self.heri.get_job_status(job_name)
        elif job_type == WAYBACK_CREATE:
            return self.wb_downloader.get_job_status(job_name)
        else:
            return "UNKNOWN"

    def _reconcile(self):
        """Poll Heritrix for RUNNING jobs and mark SUCCEEDED when all underlying job_names are no longer RUNNING."""
        running = self.state.list_running_jobs()
        for job in running:
            payload = job.get("payload") or {}
            job_names = payload.get("job_names") or []
            jtype = job.get("type")
            if not job_names:
                # No way to reconcile; mark as SUCCEEDED after a grace period?
                log.debug("no job name, mark as succeed for job %s", job["id"])
                self.state.mark_succeeded(job["id"])
                continue

            statuses = [self._get_job_status(jtype, name) for name in job_names]
            # Consider SUCCEEDED when none are RUNNING (Heritrix often reports FINISHED/PAUSED/ENDED)
            if all(s not in ("RUNNING", "UNBUILT") for s in statuses):
                log.debug("mark as succeed for job %s", job["id"])
                self.state.mark_succeeded(job["id"])

    def run_forever(self):
        max_parallel = int(self.cfg["queue"]["max_parallel_jobs"])
        reconcile_every = int(self.cfg["queue"]["reconcile_interval_seconds"])
        max_retries = int(self.cfg["queue"]["max_retries"])
        backoff = int(self.cfg["queue"]["retry_backoff_seconds"])

        log.info("JobQueue worker started: max_parallel=%d reconcile_every=%ds", max_parallel, reconcile_every)
        while not self._stop.is_set():
            time.sleep(reconcile_every)
            try:
                self._reconcile()
            except Exception:
                log.exception("Reconcile failed")

            running = self.state.count_running_jobs()
            capacity = max_parallel - running
            log.debug("Reconciled queue; currently running=%d, capacity=%d",
                      running, capacity)
            if capacity <= 0:
                continue

            jobs = self.state.fetch_next_pending(limit=capacity)
            if not jobs:
                continue

            for job in jobs:
                if self.state.job_running(job["domain"]):
                    log.info("A job for this domain is running, skip this job")
                    continue
                self.state.mark_running(job["id"])
                log.info("Dequeued -> RUNNING id=%s type=%s domain=%s", job["id"], job["type"], job["domain"])
                try:
                    self._handle_job(job)
                    # leave RUNNING; _reconcile will mark SUCCEEDED when job(s) finish
                except Exception as ex:
                    log.exception("Job %s failed in handler", job["id"])
                    self.state.mark_failed(job["id"], str(ex), max_retries=max_retries)
                    if max_retries > 0:
                        time.sleep(backoff)
        self.wb_downloader.destroy()
        log.info("JobQueue worker stopped")
