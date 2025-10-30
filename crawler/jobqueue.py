# fma/jobqueue.py
from __future__ import annotations
import threading
import time
from typing import Dict, List
from datetime import datetime
from .state import State
from .btrix_cli import BrowsertrixClient
from .wb_downloader import WBDownloader
import logging, time

log = logging.getLogger(__name__)

# Queue job types
LIVE_CRAWL = "LIVE_CRAWL"
WAYBACK_DOWNLOAD = "WAYBACK_DOWNLOAD"

class JobQueueWorker:
    def __init__(self, cfg, state: State):
        self.cfg = cfg
        self.state = state
        self.btrix = BrowsertrixClient(
            base_url=cfg["browsertrix"]["base_url"],
            username=cfg["browsertrix"]["username"],
            password=cfg["browsertrix"]["password"],
        )
        self.wb_downloader = WBDownloader(
            downloader=cfg["wb_downloader"]["downloader"],
            output_dir=cfg["wb_downloader"]["output_dir"],
            concurrency=int(cfg["wb_downloader"]["concurrency"])
        )
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def _handle_job(self, job) -> None:
        jtype = job["type"]
        url = job["url"]
        payload = job['payload'] or {}

        if jtype == LIVE_CRAWL:
            log.info("LIVE_CRAWL url=%s", url)
            payload['name'] = self.btrix.create_job(url, self.cfg["browsertrix"]["crawler_setting"])

        elif jtype == WAYBACK_DOWNLOAD:
            log.info("WAYBACK_CREATE url=%s", url)
            payload['name'] = self.wb_downloader.create_job(url)

        else:
            log.error("Unknow job type for url=%s", url)
            return
        
        self.state.update_job_payload(job["id"], payload)

    def _get_job_status(self, job_type, job_name) -> str:
        """ job status: PENDING->RUNNING->FINISHED/FAILED."""
        if job_type == LIVE_CRAWL:
            return self.btrix.get_job_status(job_name)
        elif job_type == WAYBACK_DOWNLOAD:
            return self.wb_downloader.get_job_status(job_name)
        else:
            return "UNKNOWN"

    def _reconcile(self):
        """Poll Browsertrix for RUNNING jobs and mark SUCCEEDED when all underlying job_names are no longer RUNNING."""
        running = self.state.list_running_jobs()
        for job in running:
            payload = job.get("payload") or {}
            job_names = payload.get("job_names") or []
            jtype = job.get("type")
            status = self._get_job_status(jtype, job_names)
            if status == 'FINISHED':
                self.state.mark_succeeded(job["id"])
            elif status == 'FAILED':
                self.state.mark_failed(job["id"])

    def run_forever(self):
        max_parallel = int(self.cfg["queue"]["max_parallel_jobs"])
        reconcile_every = int(self.cfg["queue"]["reconcile_interval_seconds"])

        log.info("JobQueue worker started: max_parallel=%d reconcile_every=%ds", max_parallel, reconcile_every)
        while not self._stop.is_set():
            time.sleep(reconcile_every)
            try:
                self._reconcile()
            except Exception:
                log.exception("Reconcile failed")

            running = self.state.count_running_jobs()
            capacity = max_parallel - running
            log.debug("Reconciled queue: currently running=%d, capacity=%d",
                      running, capacity)
            if capacity <= 0:
                continue

            jobs = self.state.fetch_next_pending(limit=capacity)
            if not jobs:
                continue

            for job in jobs:
                self.state.mark_running(job["id"])
                log.info("Dequeued -> RUNNING id=%s type=%s url=%s", job["id"], job["type"], job["url"])
                try:
                    self._handle_job(job)
                except Exception as ex:
                    log.exception("Job %s failed in handler", job["id"])

        self.wb_downloader.destroy()
        log.info("JobQueue worker stopped")
