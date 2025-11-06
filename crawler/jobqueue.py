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
    """ Manages job queue for crawl and download jobs. """
    def __init__(self, cfg, state: State):
        self.cfg = cfg
        self.state = state
        self.btrix = BrowsertrixClient(
            base_url=cfg["browsertrix"]["base_url"],
            username=cfg["browsertrix"]["username"],
            password=cfg["browsertrix"]["password"],
        )
        self.wb_downloader = WBDownloader(
            output_base=cfg["wb_downloader"]["output_dir"],
            concurrency=int(cfg["wb_downloader"]["concurrency"])
        )
        self._stop = threading.Event()

    def stop(self):
        """Signal the worker to stop."""
        self._stop.set()

    def _handle_job(self, job) -> None:
        jtype = job["type"]
        url = job["url"]
        job_desc = f"""nca_id:{job["nca_id"]},validation_date:{job["validation_date"]}"""
        job_name = ""
        job_link = ""

        if jtype == LIVE_CRAWL:
            log.info("LIVE_CRAWL url=%s", url)
            job_name = self.btrix.create_job(url, job_desc, self.cfg["browsertrix"]["crawler_setting"])
            job_link = f"{self.cfg['browsertrix']['base_url'].rstrip('/')}/orgs/my-organization/workflows/{job_name}"

        elif jtype == WAYBACK_DOWNLOAD:
            log.info("WAYBACK_CREATE url=%s", url)
            job_name = self.wb_downloader.create_job(url, job_desc)

        else:
            log.error("Unknow job type for url=%s", url)
            return

        self.state.update_job_info(job["id"], job_name, job_link)

    def _get_job_status(self, job_type, job_name) -> str:
        """ job status: PENDING->RUNNING->FINISHED/FAILED."""
        if job_type == LIVE_CRAWL:
            return self.btrix.get_job_status(job_name)
        elif job_type == WAYBACK_DOWNLOAD:
            return self.wb_downloader.get_job_status(job_name)
        else:
            return {"Status": "UNKNOWN"}

    def _reconcile(self):
        """Poll Browsertrix for RUNNING jobs and mark SUCCEEDED when all underlying
        job_names are no longer RUNNING."""
        running = self.state.list_running_jobs()
        for job in running:
            job_names = job.get("job_name")
            jtype = job.get("type")
            status = self._get_job_status(jtype, job_names)
            if status["status"] == 'FINISHED':
                self.state.mark_finished(job["id"], status["crawl_count"], status["file_count"])
            elif status["status"] == 'FAILED':
                self.state.mark_failed(job["id"], int(self.cfg["queue"]["max_retries"]) \
                                       if jtype == WAYBACK_DOWNLOAD else 0)
            elif status["status"] == 'RUNNING':
                # still running
                pass
            else:
                log.warning("Unknown status: %s", status["status"])

    def rebuild_job_info(self) -> List[Dict]:
        """ Rebuild job info from existing jobs in Browsertrix and WBDownloader."""
        for job in self.btrix.rebuild_job_info():
            desc = job["desc"]
            nca_id = int(desc.split(",")[0].split(":")[1]) if desc else 0
            validation_date = desc.split(",")[1].split(":")[1] if desc else ""
            self.state.add_history_job(
                job_type=LIVE_CRAWL,
                job_name=job["job_name"],
                url=job["url"],
                nca_id=nca_id,
                validation_date=validation_date,
                status=job["status"],
                crawl_count=job["crawl_count"],
                file_count=job["file_count"],
                link="" # TBD
            )

        for job in self.wb_downloader.rebuild_job_info():
            desc = job["desc"]
            nca_id = int(desc.split(",")[0].split(":")[1]) if desc else 0
            validation_date = desc.split(",")[1].split(":")[1] if desc else ""
            self.state.add_history_job(
                job_type=WAYBACK_DOWNLOAD,
                job_name=job["job_name"],
                url=job["url"],
                nca_id=nca_id,
                validation_date=validation_date,
                status=job["status"],
                crawl_count=1,
                file_count=job["file_count"],
                link="" # TBD
            )

    def run_forever(self):
        """Main loop to process job queue."""
        max_parallel = {}
        max_parallel[LIVE_CRAWL] = int(self.cfg["queue"]["max_parallel_crawl_jobs"])
        max_parallel[WAYBACK_DOWNLOAD] = int(self.cfg["queue"]["max_parallel_download_jobs"])
        reconcile_every = int(self.cfg["queue"]["reconcile_interval_seconds"])

        log.info("JobQueue worker started: max_parallel_crawl_jobs=%d, max_parallel_download_jobs=%d, reconcile_every=%ds",
                 max_parallel[LIVE_CRAWL], max_parallel[WAYBACK_DOWNLOAD], reconcile_every)

        # TODO:
        log.info("Purge all crawls from Browsertrix for testing...")
        self.btrix.purge_all_crawls()
        self.btrix.purge_all_crawlconfigs()
    
        if self.state.get_last_full_run() is None:
            # try to rebuild job info from existing jobs
            log.info("First run detected, rebuilding job info from existing jobs")
            self.rebuild_job_info()

        while not self._stop.is_set():
            time.sleep(reconcile_every)
            try:
                self._reconcile()
            except Exception:
                log.exception("Reconcile failed")

            jobs = []
            for job_type in [LIVE_CRAWL, WAYBACK_DOWNLOAD]:
                running = self.state.count_running_jobs(job_type)
                capacity = max_parallel[job_type] - running
                log.debug("Reconciled queue of %s jobs: currently running=%d, capacity=%d",
                          job_type, running, capacity)
                if capacity <= 0:
                    continue

                jobs += self.state.fetch_next_pending(job_type, capacity)

            if len(jobs) == 0:
                continue

            for job in jobs:
                self.state.mark_running(job["id"])
                log.info("Dequeued -> RUNNING id=%s type=%s url=%s",
                         job["id"], job["type"], job["url"])
                try:
                    self._handle_job(job)
                except Exception as ex:
                    log.exception("Job %s failed in handler, error: %s", job["id"], str(ex))

        self.wb_downloader.destroy()
        log.info("JobQueue worker stopped")
